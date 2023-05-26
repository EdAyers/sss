from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from functools import cache, reduce
import logging
import operator
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Type,
    TypeVar,
    Union,
    overload,
)
from typing_extensions import dataclass_transform
from .engine import Engine, engine_context
from .column import Column, col
from .q import Expr, FromClause, SelectStatement, parse_sql, parse_commands, Setter
import dxd.q as q
from .pattern import Pattern

T = TypeVar("T", bound="Schema")
S = TypeVar("S")
R = TypeVar("R")

logger = logging.getLogger("dxd")


class OrderKind(Enum):
    Ascending = "ASC"
    Descending = "DESC"


WhereClause = Union[bool, dict, tuple]


class SchemaMeta(type):
    def __getattr__(self, key):
        """If we call `User.name`, it should return the `Column` object for that."""
        if key.startswith("__"):
            raise AttributeError()
        fields = getattr(self, "__dataclass_fields__")
        if fields is None:
            # [todo] pydantic support
            raise ValueError("Remember to make Schema a dataclass")
        field = fields.get(key, None)
        if field is None:
            raise AttributeError(f"No column named {key}")
        return Column.of_field(self, field)


class Schema(metaclass=SchemaMeta):
    @classmethod
    def default_name(cls):
        return cls.__name__ + "_table"

    @classmethod
    def create_table(
        cls: Type[T],
        name: Optional[str] = None,
        engine: Optional[Engine] = None,
        references: dict[Any, "Table[Any]"] = {},
        check_schema: Literal["ignore", "raise", "clobber"] = "clobber",
    ) -> "Table[T]":
        return Table.create(
            schema=cls,
            name=name,
            engine=engine,
            references=references,
            check_schema=check_schema,
        )

    @classmethod
    def as_column(cls, item: Union[Column, str]) -> Column:
        if isinstance(item, Column):
            return item
        elif isinstance(item, str):
            fields = getattr(cls, "__dataclass_fields__")
            # [todo] pydantic
            return Column.of_field(cls, fields.get(item))
        else:
            raise TypeError(f"can't convert {item} to a table column")

    def __init__(self, **kwargs):
        raise NotImplementedError("Schema is abstract")


@dataclass
class Table(Generic[T]):
    name: str
    connection: Engine
    schema: "Type[T]"

    def __len__(self):
        smt = parse_commands(f"SELECT COUNT(*) FROM {self.name}")
        c = self.connection.execute_expr(smt)
        return c.fetchone()[0]

    def drop(self, not_exists_ok: bool = True):
        """Drops the table.

        Note that once this is called subsequent queries to the table will error."""
        ne = "IF EXISTS " if not_exists_ok else ""
        self.connection.execute(f"DROP TABLE {ne}{self.name};")

    def does_exist(self):
        """Returns true if the table exists on the given sqlite connection.

        This returns false when you have dropped the table."""
        cur = self.connection.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.name}';"
        )
        return bool(cur.fetchone())

    def _mk_where_clause(self, where: WhereClause) -> Optional[q.WhereClause]:
        if where is True:
            return None
        if isinstance(where, dict):
            e = [self.schema.as_column(k) == v for k, v in where.items()]
            e = reduce(operator.and_, e)
        elif isinstance(where, tuple):
            e = reduce(operator.and_, where)
        elif isinstance(where, Expr):
            e = where
        else:
            raise TypeError("where clause must be a dict, tuple, or Expr")
        e = q.WhereClause.create(where)
        return e

    def primary_key_pattern(self) -> Pattern:
        pcs = [c for c in columns(self) if c.primary]
        if len(pcs) == 1:
            return Pattern(pcs[0])
        return Pattern(tuple(Pattern(c) for c in pcs))

    def pattern(self) -> Pattern[T]:
        """Return a pattern sending table entries to instances of the self.schema class."""
        cs = list(columns(self))
        return Pattern({c.name: Pattern(c) for c in cs}).map(lambda d: self.schema(**d))

    @overload
    def select(
        self,
        *,
        where: WhereClause = True,
        order_by: Optional[Any] = None,
        descending=False,
        distinct=False,
        limit: Optional[int] = None,
    ) -> Iterator[T]:
        ...

    @overload
    def select(
        self,
        *,
        where: WhereClause = True,
        select: S,
        order_by: Optional[Any] = None,
        descending=False,
        distinct=False,
        limit: Optional[int] = None,
    ) -> Iterator[S]:
        ...

    def select(self, *, where=True, select=None, order_by: Optional[Any] = None, descending=False, limit: Optional[int] = None, distinct=False):  # type: ignore
        p: Pattern = Pattern(select) if select is not None else self.pattern()

        core = q.SelectCore(
            smode="DISTINCT" if distinct else None,
            columns=[q.AliasedExpr(expr=x) for x in p.items],
            from_clause=FromClause.of_table_name(self.name),
            where_clause=self._mk_where_clause(where),
        )
        stmt = q.SelectStatement(
            core=[core],
        )
        if order_by is not None:
            stmt.order_by_clause = q.OrderByClause(
                terms=[
                    q.OrderingTerm(
                        expr=q.expr(order_by),
                        dir="DESC" if descending else "ASC",
                    )
                ]
            )
        if limit is not None:
            stmt.limit_clause = q.LimitClause(limit=q.expr(limit))
        xs = self.connection.execute_expr(stmt)
        return map(p.outfn, xs)

    def sum(self, col, where=True) -> float:
        c = self.schema.as_column(col)
        statement = q.parse_statement(f"SELECT SUM( ? ) FROM {self.name}", c)
        assert isinstance(statement, q.SelectStatement)
        c = statement.core[0]
        assert isinstance(c, q.SelectCore)
        c.where_clause = self._mk_where_clause(where)
        xs = self.connection.execute_expr(statement)
        return next(iter(xs))[0] or 0  # type: ignore

    @overload
    def insert_one(self, item: T, *, or_ignore=False) -> None:
        ...

    @overload
    def insert_one(self, item: T, *, returning: S, or_ignore=False) -> S:
        ...

    def insert_one(self, item, *, returning=None, or_ignore=False):
        if not isinstance(item, self.schema):
            raise TypeError(f"Expected {self.schema.__name__}, got {type(item)}")
        assert isinstance(item, self.schema)
        if returning is not None:
            r = self.insert_many([item], returning, or_ignore=or_ignore)
            return next(iter(r))
        else:
            self.insert_many(items=[item], returning=returning, or_ignore=or_ignore)

    @overload
    def insert_many(self, items: Iterable[T], or_ignore=False) -> None:
        ...

    @overload
    def insert_many(
        self, items: Iterable[T], returning: S, or_ignore=False
    ) -> Iterable[S]:
        ...

    def insert_many(self, items, returning=None, or_ignore=False):  # type: ignore
        items = list(items)
        assert all(isinstance(x, self.schema) for x in items)
        cs = list(columns(self))
        values: list[q.BracketList[Expr]] = [
            q.BracketList(
                [q.UnnamedPlaceholder(c.adapt(getattr(x, c.name))) for c in cs]
            )
            for x in items
        ]

        statement = q.InsertStatement(
            table_name=q.Identifier(self.name),
            columns=q.BracketList(cs),
            values=q.InsertValues(
                values=values, upsert=q.ON_CONFLICT_DO_NOTHING if or_ignore else None
            ),
        )
        if returning is not None:
            p = Pattern(returning)
            statement.returning_clause = q.ReturningClause(
                items=[q.AliasedExpr(expr=x) for x in p.items]
            )
            cur = self.connection.execute_expr(statement)
            return map(p.outfn, cur)
        else:
            cur = self.connection.execute_expr(statement)
            cur.close()

    @overload
    def select_one(
        self,
        *,
        where: WhereClause = True,
        select: S,
        order_by: Optional[Any] = None,
        descending=False,
    ) -> Optional[S]:
        ...

    @overload
    def select_one(
        self,
        *,
        where: WhereClause = True,
        order_by: Optional[Any] = None,
        descending=False,
    ) -> Optional[T]:
        ...

    def select_one(
        self,
        *,
        where: WhereClause = True,
        select=None,
        order_by: Optional[Any] = None,
        descending=False,
    ):
        return next(
            self.select(
                where=where,
                select=select,
                limit=1,
                order_by=order_by,
                descending=descending,
            ),
            None,
        )

    def has(self, where: WhereClause):
        return self.select_one(where=where) is not None

    @overload
    def update(self, values, *, where: bool = True, returning: S) -> Iterator[S]:
        """Run an UPDATE query, returning the pattern given by 'returning'."""
        ...

    @overload
    def update(self, values, *, where: bool = True) -> int:
        """Run an UPDATE query on the object. Returns the number of records that were updated."""
        ...

    def update(self, values, where=True, returning=None):  # type: ignore
        # [todo] if 'where : T', set where to be T's primary key.

        setters: q.Setters = [
            q.Setter(col=q.Identifier(self.schema.as_column(k).name), value=q.expr(v))
            for k, v in values.items()
        ]

        statement = q.UpdateStatement(
            name=q.QualifiedTableName(name=q.Identifier(self.name)),
            setters=setters,
            where_clause=self._mk_where_clause(where),
        )

        if returning is not None:
            p = Pattern(returning)
            statement.returning_clause = q.ReturningClause(
                items=[q.AliasedExpr(expr=x) for x in p.items]
            )
            xs = self.connection.execute_expr(statement)
            return map(p.outfn, xs)
        else:
            cur = self.connection.execute_expr(statement)
            if self.connection.mode == "sqlite":
                i = cur.execute("SELECT changes();").fetchone()[0]
                return i
            else:
                return cur.rowcount

    def delete(self, where: WhereClause):
        assert isinstance(where, Expr)
        statement = q.DeleteStatement(
            table_name=q.QualifiedTableName(name=q.Identifier(self.name)),
            where_clause=self._mk_where_clause(where),
        )
        self.connection.execute_expr(statement)

    def clear(self):
        q = f"DELETE FROM {self.name};"
        self.connection.execute(q)

    @classmethod
    def create(
        cls,
        schema: Type[T],
        name: Optional[str] = None,
        engine: Optional[Engine] = None,
        references: dict[Any, "Table[Any]"] = {},
        check_schema: Literal["ignore", "raise", "clobber"] = "clobber",
    ):
        # if clobber is true then if the table exists but the schema has changed we
        # just brutally wipe everything.
        # [todo] validate column names and table name.
        # [todo] migrations?
        if not is_dataclass(schema):
            raise TypeError(
                f"{schema.__name__} is not a dataclass. "
                f"dxd requires all Schema subclasses to be dataclasses."
            )
            # [todo] pydantic support one day.
        assert issubclass(schema, Schema)
        engine = engine or engine_context.get()
        name = name or schema.default_name()  # type: ignore

        fields = [c.sql_schema for c in columns(schema)]
        if not any(c.primary for c in columns(schema)):
            raise TypeError(
                f"at least one of the fields in {schema.__name__} should be labelled as primary: `= col(primary = True)`"
            )
        ks = ", ".join([c.name for c in columns(schema) if c.primary])
        fields.append(f"PRIMARY KEY ({ks})")
        for c in columns(schema):
            fk = c.foreign_key
            if fk is not None:
                table: Optional[Table] = references.get(c, references.get(c.name, None))
                if table is None:
                    raise ValueError(
                        f"no reference table found for foreign key {repr(c)}"
                    )
                if isinstance(fk, Column):
                    if table.schema != fk.schema:
                        raise ValueError(
                            f"Incompatible foreign key {repr(fk)} in {table}"
                        )
                    if fk.type != c.type:
                        raise TypeError(
                            f"types {repr(fk)} : {fk.type} and {repr(c)} : {c.type} do not match"
                        )
                    fields.append(
                        f"FOREIGN KEY ({c.name}) REFERENCES {table.name} ({fk.name}) ON DELETE CASCADE"
                    )
                elif fk is True:
                    # get the primary key
                    p = table.primary_key_pattern()
                    t = q.sql(list(p.items))
                    logger.debug(f"Guessing template {t} for table {table.name}")
                    fields.append(
                        f"FOREIGN KEY ({c.name}) REFERENCES {table.name} ({t}) ON DELETE CASCADE"
                    )
                else:
                    raise TypeError(f"unknown foreign key {repr(fk)}")
        fields = ",\n  ".join(fields)

        if check_schema != "ignore":
            schema_table = get_schema_table(engine)
            schemas = list(
                schema_table.select(
                    where=(SchemaRecord.table_name == name)
                    & (SchemaRecord.fields != fields)
                )
            )
            if len(schemas) > 0:
                if check_schema == "raise":
                    raise RuntimeError(
                        f"Schema for {name} has changed, please migrate the table and try again."
                    )
                elif check_schema == "clobber":
                    logger.warning(
                        f"Schema for {name} already exists, clobbering. Data is lost."
                    )
                    # [todo] add a feature where table names are prefixed with a schema version
                    # just keep multiple incompatible tables.
                    # [todo] migration stuff will eventually go here.
                    # [todo] cascading?
                    engine.execute(f"DROP TABLE {name};")
                    schema_table.delete(where=(SchemaRecord.table_name == name))
            schema_table.insert_one(
                SchemaRecord(table_name=name, fields=fields), or_ignore=True
            )

        statement_string = f"CREATE TABLE IF NOT EXISTS {name} (\n  {fields}\n);"
        # [todo] just construct the create table statement directly rather than reparsing it.
        statement = q.parse_sql(q.CreateTableStatement, statement_string)
        engine.execute_expr(statement)
        engine.commit()
        return cls(name=name, connection=engine, schema=schema)  # type: ignore


# [todo] Table should not be instantiated


def columns(x: Union[Table[T], Schema, Type[Schema]]) -> Iterable[Column]:
    """Get the columns of a table or schema."""
    if isinstance(x, Table):
        x = x.schema
    if isinstance(x, Schema):
        x = type(x)
    assert issubclass(x, Schema)
    if x is Schema:
        raise TypeError("columns() must be called on a strict subclass of Schema.")
    assert is_dataclass(x), f"Expected dataclass, got {type(x)}"
    return [Column.of_field(x, f) for f in fields(x)]


@dataclass
class SchemaRecord(Schema):
    """Used to store schemas of tables in the database. This is used for migrations."""

    table_name: str = col(primary=True)
    fields: str = col(primary=True)


@cache
def get_schema_table(engine: Engine) -> Table[SchemaRecord]:
    table = SchemaRecord.create_table(
        "_dxd_schema_table", engine, check_schema="ignore"
    )
    return table


"""
# [todo] Notes on what schemas are for

I have this idea to have a single decorator called `@schema` which does everything.
The idea is you are able to write

```
@schema
class User:
    name : str
    email : str = col(primary = True)
    birthday : date
```


Things that a schema gives you are:
- Type-safe column names, you can write things like `users[User.name == "george"]`.
  Otherwise you need a way of identifying the 'name' Column expression.
- way to easily add extra metadata to columns (primary key, unique, foreign key, sql-implemented validation)
- maybe also define indexes?

So what does `@schema` do?
- Wrap `User` in `@dataclass`,
- Read off column metadata like 'primary' etc.
- Add Column classvars to `User` for each column,

So I think we can get away without a `Schema` mixin.
Then we can decorate other things with `@schema` like pydantic models.
`@schema` just sets a `__dxd_columns__` attribute on the class and column classvars if appropriate.

I also want it to be possible to create tables without needing to use my special schema thing.

You can do `Table.create(schema = X)` where:
    - `X = dict[K, V]`
    - `X` is a dataclass
    - `X <: BaseModel`

The only thing schema gives you is the ability to do the convenient column name stuff and column metadata.
We can still get the column metadata using `users[users.name == "george"]`, but we won't be able to give the type of 'users.name'.

# Query builders

The next phase of dxd will use pandas-like constructions to build queries.
This is similar to what some of these big-data tools like Spark do.

When you write `users[users.name == "george"]` this is converted to a `View[User]` object.
`users.name : Series[bool]`.

The difference between a View and a Series is that a view has multiple columns that you can project to.
But a series does not. Series[User] is like a stream of User objects.

Each view and series is backed by a SQL query expression (currently reprsented as `Expr`).

- `users.name` ↝ `SELECT name FROM users`
- `users.name == 'george'` ↝ `SELECT (name == 'george') FROM users`
- `users[users.name == 'george']` ↝ `SELECT * FROM users WHERE (name == 'george')`
- `users[users.name == 'george'].birthday` ↝ `SELECT birthday FROM users WHERE (name == 'george')`

Writing the query builder is going to be tricky, but definitely seems possible.
There will be an interesting 'query-IL' structure that can be compiled to SQL.

## Keytype management

The key difference from pandas is that you can't assume an order on the rows,
they are just indexed by some fintype that doesn't have a natural ordering.
When you call `iter` on a view or series, an ordering is chosen but you aren't guaranteed to
get the same ordering each time.

Treating these 'indexes' or 'parameters' more abstractly is a good thing because it makes it
harder to accidentally perform operations that don't make sense. I won't call them indexes because that
already means something in databases. Instead lets call them keytype.

For an array of length n, the keytype is `N = Fin(n)`.
If we then argsort this array on `s`, a better keytype is not `N` again but instead an isomorphic type
$M_{x, s}$. Then we write `argsort(x, s) : M ⇒ N`.
For a table, the keytype is the set of primary keys.
Also distinct keytypes, for filtering, grouping, joins, broadcasting.

Suppose you filter your users by name, you now have a new index which is a subindex of the original
table's index. Keeping track of the keytypes amounts to having dependent type theory, but it means
that we don't accidentally perform an operation on two arrays that have the same size but are sorted differently etc.
It also means that we don't confuse iterations of tables with the abstract, uniterated table.
Even if we only keep track of keytypes in our heads they are useful.

"""
