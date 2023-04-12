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
from .engine import Engine, engine_context
from .column import Column, col
from .expr import Expr
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
        # if clobber is true then if the table exists but the schema has changed we
        # just brutally wipe everything.
        # [todo] validate column names and table name.
        # [todo] migrations?
        if not is_dataclass(cls):
            raise TypeError(
                f"{cls.__name__} is not a dataclass. "
                f"dxd requires all Schema subclasses to be dataclasses."
            )
        engine = engine or engine_context.get()
        name = name or cls.default_name()  # type: ignore

        fields = [c.sql_schema for c in columns(cls)]
        if not any(c.primary for c in columns(cls)):
            raise TypeError(
                f"at least one of the fields in {cls.__name__} should be labelled as primary: `= col(primary = True)`"
            )
        ks = ", ".join([c.name for c in columns(cls) if c.primary])
        fields.append(f"PRIMARY KEY ({ks})")
        for c in columns(cls):
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
                    e = p.to_expr()
                    assert len(e.values) == 0, "pattern had values"
                    t = e.template
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

        q = f"CREATE TABLE IF NOT EXISTS {name} (\n  {fields}\n);"
        engine.execute(q)
        engine.commit()
        return Table(name=name, connection=engine, schema=cls)  # type: ignore

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


@dataclass
class Table(Generic[T]):
    name: str
    connection: Engine
    schema: "Type[T]"

    def __len__(self):
        c = self.connection.execute(f"SELECT COUNT(*) FROM {self.name} ; ")
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

    def _mk_where_clause(self, where: WhereClause) -> "Expr":
        if where is True:
            return Expr.empty()
        if isinstance(where, dict):
            e = [self.schema.as_column(k) == v for k, v in where.items()]
            e = Expr.binary("AND", e, precedence=2)
        else:
            assert isinstance(where, Expr)
            e = where
        return Expr("WHERE ?", [e])

    def primary_key_pattern(self) -> Pattern:
        pcs = [c for c in columns(self) if c.primary]
        if len(pcs) == 1:
            return Pattern(pcs[0])
        return Pattern(tuple(Pattern(c) for c in pcs))

    def pattern(self) -> Pattern[T]:
        cs = list(columns(self))

        def blam(d: dict) -> T:
            return self.schema(**d)

        return Pattern({c.name: Pattern(c) for c in cs}).map(blam)

    def where_to_expr(self, where: WhereClause) -> Expr:
        if where is True:
            return Expr.empty()
        elif isinstance(where, dict):
            kvs = reduce(
                operator.and_, [self.schema.as_column(k) == v for k, v in where.items()]
            )
            return kvs
        elif isinstance(where, tuple):
            return reduce(operator.and_, where)
        else:
            return Expr(where)

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
        distinct_q = "DISTINCT " if distinct else ""
        query = Expr(f"SELECT {distinct_q}?\nFROM {self.name} ", [p.to_expr()])
        if where is not True:
            query = Expr("?\n?", [query, self._mk_where_clause(where)])
        if order_by is not None:
            asc = "DESC" if descending else "ASC"
            query = Expr(f"?\nORDER BY ? {asc}", [query, order_by])
        if limit is not None:
            query = Expr(f"?\nLIMIT {limit}", [query])
        xs = self.connection.execute_expr(query)
        return map(p.outfn, xs)

    def sum(self, col, where=True) -> float:
        c = self.schema.as_column(col)
        query = Expr(f"SELECT SUM({c.name}) \nFROM {self.name} ", [])
        if where is not True:
            query = Expr("?\n?", [query, self._mk_where_clause(where)])
        xs = self.connection.execute_expr(query)
        return next(iter(xs))[0] or 0

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
        qfs = ", ".join(c.name for c in cs)
        qqs = ", ".join("?" for _ in cs)
        q = f"INSERT INTO {self.name} ({qfs}) VALUES ({qqs}) "
        if or_ignore:
            q += "ON CONFLICT DO NOTHING "
        if returning is not None:
            adapt = engine_context.get().adapt
            p = Pattern(returning)
            rq = Expr("RETURNING ? ;", [p.to_expr()])
            vs = [
                tuple(
                    [c.adapt(getattr(item, c.name)) for c in cs]
                    + list(map(adapt, rq.values))
                )
                for item in items
            ]
            q = q + rq.template
            # [note] RETURNING keyword is not supported for executemany()
            return [p.outfn(self.connection.execute(q, v).fetchone()) for v in vs]
        else:
            q += ";"
            vs = [tuple(c.adapt(getattr(item, c.name)) for c in cs) for item in items]
            cursor = self.connection.executemany(q, vs)
            cursor.close()
            return

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
        def mk_setter(key, value) -> "Expr":
            key = self.schema.as_column(key)
            return Expr(f"{key.name} = ?", [value])

        setters = Expr.binary(", ", [mk_setter(k, v) for k, v in values.items()])
        t = "UPDATE"
        query = Expr(f"{t} {self.name} SET ? ", [setters])
        if where is not True:
            assert isinstance(where, Expr)
            query = Expr("?\nWHERE ?", [query, where])
        if returning is not None:
            p = Pattern(returning)
            query = Expr("?\nRETURNING ?", [query, p.to_expr()])
            xs = self.connection.execute_expr(query)
            return map(p.outfn, xs)
        else:
            cur = self.connection.execute_expr(query)
            if self.connection.mode == "sqlite":
                i = cur.execute("SELECT changes();").fetchone()[0]
                return i
            else:
                return cur.rowcount

    def delete(self, where: WhereClause):
        assert isinstance(where, Expr)
        q = Expr(f"DELETE FROM {self.name} \nWHERE ?", [where])
        self.connection.execute_expr(q)

    def clear(self):
        q = f"DELETE FROM {self.name};"
        self.connection.execute(q)


# [todo] Table should not be instantiated


def columns(x) -> Iterable[Column]:
    if isinstance(x, Table):
        x = x.schema
    assert x is not Schema
    assert issubclass(x, Schema)
    assert is_dataclass(x), f"Expected dataclass, got {type(x)}"
    return [Column.of_field(x, f) for f in fields(x)]


@dataclass
class SchemaRecord(Schema):
    table_name: str = col(primary=True)
    fields: str = col(primary=True)


@cache
def get_schema_table(engine: Engine) -> Table[SchemaRecord]:
    table = SchemaRecord.create_table(
        "_dxd_schema_table", engine, check_schema="ignore"
    )
    return table
