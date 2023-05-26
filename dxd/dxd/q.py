from types import UnionType
import typing
from typing import (
    Optional as O,
    Any,
    Literal as L,
    Union as U,
    Generic,
    TypeVar,
    Type,
    Callable,
    ClassVar,
    TypeAlias,
)
from enum import Enum
from miniscutil.type_util import as_optional, as_literal
import dataclasses
import contextlib
import shlex
import re
import functools
import itertools
from abc import ABC, abstractmethod
import warnings
from typing_extensions import dataclass_transform
from dxd.parser import ParseState, run_parser, is_word, tokenize
from miniscutil.type_util import is_optional, as_literal
import logging
from contextvars import ContextVar
from miniscutil.misc import onectx, interlace, newctx
from miniscutil import Current
from collections import Counter
import operator
from numbers import Number

logger = logging.getLogger("dxd")

T = TypeVar("T")

# [todo] placeholder ctx should live on parse state: can be backtracked.


class PlaceholderContext:
    args: list[Any]
    kwargs: dict[str, Any]
    names: Counter[str]

    def __init__(self, args=None, kwargs=None):
        self.args = args or []
        self.kwargs = kwargs or {}
        self.names = Counter()

    def add_arg(self, x: Any):
        self.args.append(x)

    def pop_arg(self):
        assert len(self.args) > 0
        return self.args.pop(0)

    def get_kwarg(self, k):
        return self.kwargs[k]

    def add_kwarg(self, k: str, v: Any):
        # [todo] better detection for injections
        assert " " not in k
        if k in self.kwargs:
            if v != self.kwargs[k]:
                self.names[k] += 1
                n = self.names[k]
                k2 = f"{k}_{n}"
                logger.debug(f"deduplicating {k} → {k2}")
                assert k2 not in self.kwargs
                k = k2
        self.kwargs[k] = v
        return k

    def __enter__(self):
        if ph_ctx.get(None) is not None:
            raise RuntimeError("PlaceholderContext already active")
        assert not hasattr(self, "_token")
        self._token = ph_ctx.set(self)
        return self

    def __exit__(self, *args):
        assert hasattr(self, "_token")
        ph_ctx.reset(self._token)
        del self._token


ph_ctx: ContextVar[PlaceholderContext] = ContextVar("ph_ctx")


def parse_sql(t: Type[T], input: str, *placeholder_args, **placeholder_kwargs) -> T:
    pc = PlaceholderContext(placeholder_args, placeholder_kwargs)
    with newctx(ph_ctx, pc):
        result = run_parser(t, input)
    assert len(pc.args) == 0, f"Unused placeholder args: {pc.args}"
    return result


@functools.singledispatch
def sql(x: Any) -> str:
    """Converts your object to a SQL string."""
    with onectx(ph_ctx, PlaceholderContext):
        if hasattr(x, "__sql__"):
            return x.__sql__()
        raise NotImplementedError(f"Cannot sql {x}")


@sql.register(list)
def _sql_list(x: list):
    return ", ".join(map(sql, x))


@sql.register(tuple)
def _sql_tuple(x: tuple):
    return " ".join(map(sql, x))


@sql.register(type(None))
def _sql_none(x: None):
    return ""


@sql.register(str)
def _sql_str(x: str):
    if " " in x:
        warnings.warn(
            f"Unescaped space in string '{x}' detected. Did you mean to use a string literal?"
        )
    return x  # escaping is done on string literals.


@sql.register(int)
def _sql_int(x: int):
    return str(x)


@dataclasses.dataclass
class SqlClauseField:
    name: str
    type: Type
    default: Any = dataclasses.MISSING
    default_factory: Any = dataclasses.MISSING


@dataclass_transform(kw_only_default=True)
def sql_clause(cls: Type[T]) -> Type[T]:
    ParseState.type_registry[cls.__name__] = cls
    cls_annotations = cls.__dict__.get("__annotations__", {})
    fields = []
    for name, type in cls_annotations.items():
        if typing.get_origin(type) is ClassVar:
            continue
        default = getattr(cls, name, dataclasses.MISSING)
        if isinstance(default, dataclasses.Field):
            raise NotImplementedError("field objects not supported yet")
        fields.append(SqlClauseField(name, type, default, dataclasses.MISSING))

    for f in fields:
        if (
            f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
        ):
            if is_optional(f.type):
                f.default = None
                setattr(cls, f.name, None)
            elif xs := as_literal(f.type):
                if len(xs) == 1:
                    x = xs[0]
                    f.default = x
                    setattr(cls, f.name, f.default)

    def init(self, *args, **kwargs):
        n = len(args)
        assert n <= len(fields)
        for i in range(n):
            setattr(self, fields[i].name, args[i])
        rest = fields[n:]
        for field in rest:
            v = kwargs.pop(field.name, field.default)
            if v is dataclasses.MISSING:
                raise ValueError(f"missing required argument {field.name}")
            # [todo] assert right type
            setattr(self, field.name, v)
        assert len(kwargs) == 0

    setattr(cls, "__init__", init)

    def __parse__(cls, p: ParseState):
        types = [f.type for f in fields]
        vs = p.parse(tuple[(*types,)])  # type: ignore
        return cls(*vs)

    def __sql__(self):
        acc = []
        for field in fields:
            v = getattr(self, field.name)
            if typing.get_origin(field.type) is L:
                assert isinstance(v, str)
                acc.append(str(v))
            elif v is not None:
                acc.append(sql(v))
        return " ".join(acc)

    if not hasattr(cls, "__parse__"):
        setattr(cls, "__parse__", classmethod(__parse__))
    if not hasattr(cls, "__sql__"):
        setattr(cls, "__sql__", __sql__)
    return cls


def with_brackets(x, prec=0):
    """Figures out the precedence of x and adds brackets if necessary."""
    xp = getattr(x, "precedence", 12)
    if xp < prec:
        return f"({sql(x)})"
    else:
        return sql(x)


class Expr(ABC):
    """Baseclass for SQL expressions."""

    @classmethod
    def __parse__(cls, p: ParseState):
        return parse_expr(p, 0)

    @abstractmethod
    def __sql__(self):
        raise NotImplementedError()

    def __add__(self, other):
        return OpRegistry.current().create("+", self, other)

    def __radd__(self, other):
        return OpRegistry.current().create("+", other, self)

    def __and__(self, other):
        return OpRegistry.current().create("AND", self, other)

    def __or__(self, other):
        return OpRegistry.current().create("OR", self, other)

    def __eq__(self, other):
        return OpRegistry.current().create("==", self, other)

    def __ne__(self, other):
        return OpRegistry.current().create("!=", self, other)

    def __not__(self):
        return OpRegistry.current().create("NOT", self)

    def __bool__(self):
        raise ValueError(
            f"{self} is not a boolean expression (please use & and | instead of 'and' and 'or')"
        )


@functools.singledispatch
def expr(item: Any) -> Expr:
    """Attempt to cast the given item to a SQL expression."""
    return UnnamedPlaceholder(item)


@expr.register(str)
def _str_expr(item: str):
    return UnnamedPlaceholder(item)


@expr.register(int)
def _int_expr(item: int):
    return UnnamedPlaceholder(item)


@expr.register(Expr)
def _expr_expr(item: Expr):
    return item


@expr.register(Enum)
def _expr_enum(item: Enum):
    return UnnamedPlaceholder(item.value)


@dataclasses.dataclass
class NamedPlaceholder(Expr):
    name: str
    value: Any

    def __sql__(self):
        k = ph_ctx.get().add_kwarg(self.name, self.value)
        return f":{k}"

    @classmethod
    def __parse__(cls, p: ParseState):
        p.take(":")
        name = p.next()
        assert is_word(name)
        value = dataclasses.MISSING
        if pc := ph_ctx.get(None):
            value = pc.get_kwarg(name)
            if isinstance(value, Expr):
                # [todo] should be isinstance(value, Sql)
                return value
        return cls(name=name, value=value)


@dataclasses.dataclass
class UnnamedPlaceholder(Expr):
    value: Any

    def __str__(self):
        if self.value is dataclasses.MISSING:
            return "?"
        else:
            return f"⟨{self.value}⟩"

    def __sql__(self):
        ph_ctx.get().add_arg(self.value)
        return f"?"

    @classmethod
    def __parse__(cls, p: ParseState):
        p.take("?")  # [todo] other placeholders
        value = dataclasses.MISSING
        if pc := ph_ctx.get(None):
            value = pc.pop_arg()
        return cls(value=value)


class QuotedString(str, Expr):
    def __sql__(self):
        # [todo] use the sqlite quoting function
        # doesn't seem to be possible without using placeholders and executing?
        return shlex.quote(str(self))

    @classmethod
    def __parse__(cls, p: ParseState):
        item = p.next()
        qs = ['"', "'"]
        for q in qs:
            if item.startswith(q) and item.endswith(q):
                # [todo] unescaping?
                return item.strip(q)
        raise ValueError(f"Expected quoted string, got {item}")


@sql.register(QuotedString)
def _quoted_string_sql(item: QuotedString):
    return item.__sql__()


class IntLiteral(int, Expr):
    def __sql__(self):
        return str(self)

    @classmethod
    def __parse__(cls, p):
        item = p.next()
        sign = 1
        if item == "-":
            sign = -1
            item = p.next()
        if item == "+":
            item = p.next()
        assert re.match(r"^\d+$", item)
        return sign * int(item)


sql.register(IntLiteral, lambda x: x.__sql__())

FixType = L["infix", "postfix", "prefix"]


@dataclasses.dataclass
class Op:
    sql_name: str
    python_name: O[str]
    python_fn: Callable
    sql_precedence: int
    fix: FixType
    arg_types: list[Type]
    return_type: Type

    def try_parse(self, p: ParseState):
        return p.try_take(self.sql_name)

    def sql(self, args):
        args = [with_brackets(a, self.sql_precedence) for a in args]
        if self.fix == "infix":
            l, r = args
            return f"{l} {self.sql_name} {r}"
        elif self.fix == "postfix":
            (l,) = args
            return f"{l} {self.sql_name}"
        elif self.fix == "prefix":
            (r,) = args
            return f"{self.sql_name} {r}"
        else:
            raise NotImplementedError(self.fix)


class OpInstance(Expr):
    op: Op
    args: list[Expr]

    def __init__(self, op: Op, *args):
        self.op = op
        assert len(args) == len(op.arg_types)
        self.args = [expr(arg) for arg in args]

    def __sql__(self):
        return self.op.sql(self.args)


class OpRegistry(Current):
    def __init__(self, ops: list[Op]):
        self.ops = ops

    @classmethod
    def default(cls):
        return cls(OPS)

    def get_ops(self, fix: FixType, min_precedence: int = -1):
        for op in self.ops:
            if op.fix == fix and op.sql_precedence > min_precedence:
                yield op

    def get(self, sql_name: str):
        for op in self.ops:
            if op.sql_name == sql_name:
                return op
        raise LookupError()

    def get_prec(self, sql_name: str):
        for op in self.ops:
            if op.sql_name == sql_name:
                return op.sql_precedence
        raise LookupError()

    def create(self, sql_name: str, *args):
        if len(args) == 2:
            ops = [x for x in self.get_ops("infix") if x.sql_name == sql_name]
            assert len(ops) == 1
            return OpInstance(ops[0], *args)
        return OpInstance(self.get(sql_name), *args)


OPS = [
    Op("NOT", "~", operator.not_, 2, "prefix", [bool], bool),
    # Op('~', '~', ?, 11)
    Op("+", "+", operator.pos, 11, "prefix", [Number], Number),
    Op("-", "-", operator.neg, 11, "prefix", [Number], Number),
    # "||": 9,
    # "->": 9,
    # "->>": 9,
    Op("*", "*", operator.mul, 8, "infix", [Number, Number], Number),
    Op("/", "/", operator.truediv, 8, "infix", [Number, Number], Number),
    Op("%", "%", operator.mod, 8, "infix", [Number, Number], Number),
    Op("+", "+", operator.add, 7, "infix", [Number, Number], Number),
    Op("-", "-", operator.sub, 7, "infix", [Number, Number], Number),
    Op("&", "&", operator.and_, 6, "infix", [Number, Number], Number),
    Op("|", "|", operator.or_, 6, "infix", [Number, Number], Number),
    Op("<<", "<<", operator.lshift, 6, "infix", [Number, Number], Number),
    Op(">>", ">>", operator.rshift, 6, "infix", [Number, Number], Number),
    Op("<", "<", operator.lt, 4, "infix", [Number, Number], bool),
    Op(">", ">", operator.gt, 4, "infix", [Number, Number], bool),
    Op("<=", "<=", operator.le, 4, "infix", [Number, Number], bool),
    Op(">=", ">=", operator.ge, 4, "infix", [Number, Number], bool),
    Op("=", "=", operator.eq, 3, "infix", [Any, Any], bool),
    Op("==", "==", operator.eq, 3, "infix", [Any, Any], bool),
    Op("!=", "!=", operator.ne, 3, "infix", [Any, Any], bool),
    Op("<>", "<>", operator.ne, 3, "infix", [Any, Any], bool),
    Op("IS", "is", operator.is_, 3, "infix", [Any, Any], bool),
    Op("IS NOT", "is not", operator.is_not, 3, "infix", [Any, Any], bool),
    # Op('IS DISTINCT FROM', 'is_distinct_from', operator.is_not, 3, "infix", [Any, Any], bool),
    # Op('IS NOT DISTINCT FROM', 'is_not_distinct_from', operator.is_not, 3, "infix", [Any, Any], bool),
    Op("AND", "&", operator.and_, 1, "infix", [bool, bool], bool),
    Op("OR", "|", operator.or_, 1, "infix", [bool, bool], bool),
    Op("ISNULL", None, lambda x: x is None, 3, "postfix", [Any], bool),
    Op("NOT NULL", None, lambda x: x is not None, 3, "postfix", [Any], bool),
    Op("NOTNULL", None, lambda x: x is not None, 3, "postfix", [Any], bool),
]


def parse_atom(p: ParseState) -> Expr:
    return p.parse(
        U[
            Identifier,
            IntLiteral,
            QuotedString,
        ]
    )


class PrecedenceError(Exception):
    pass


@dataclasses.dataclass
class ExprTuple(Expr):
    items: list[Expr]

    @classmethod
    def __parse__(cls, p: ParseState):
        items = p.parse(BracketList[Expr])
        return cls(items=items)

    def __sql__(self):
        return f"({', '.join(sql(x) for x in self.items)})"


def parse_expr(p: ParseState, l_prec: int = 0) -> Expr:
    if p.try_take("("):
        xs = p.parse(list[Expr])
        p.take(")")
        assert len(xs) > 0
        if len(xs) == 1:
            return xs[0]
        else:
            return ExprTuple(xs)
    if r := p.try_parse(tuple[str, L["("]]):
        # function application
        raise NotImplementedError()
    if p.try_take("CAST"):
        raise NotImplementedError()
    if p.try_take("CASE"):
        raise NotImplementedError()
    for op in OpRegistry.current().get_ops("prefix", l_prec):
        if op.try_parse(p):
            right = parse_expr(p, op.sql_precedence)
            return OpInstance(op, right)
    # [todo] function name
    #
    item = parse_atom(p)
    return parse_op(p, item, l_prec)


Globbie = tuple[O[L["NOT"]], L["GLOB", "REGEXP", "MATCH", "LIKE"]]


def parse_op(p: ParseState, left, l_prec: int) -> Expr:
    if p.can_take(")"):
        return left
    for op in OpRegistry.current().get_ops("infix", l_prec):
        if op.try_parse(p):
            right = parse_expr(p, op.sql_precedence)
            return OpInstance(op, left, right)
    for op in OpRegistry.current().get_ops("postfix", l_prec):
        if op.try_parse(p):
            return OpInstance(left)
    if p.try_take("COLLATE", case_sensitive=False):
        raise NotImplementedError()
    if p.try_parse(Globbie):
        raise NotImplementedError()
    if p.try_parse(tuple[O[L["NOT"]], L["BETWEEN", "IN"]]):
        raise NotImplementedError()
    return left


class Identifier(Expr):
    parts: list[str]

    def __init__(self, parts: list[str] | str):
        if isinstance(parts, str):
            parts = parts.split(".")
        assert all(map(is_word, parts))
        self.parts = parts

    def __sql__(self):
        return ".".join(self.parts)

    @classmethod
    def __parse__(cls, p):
        parts = []
        parts.append(p.take_word())
        while p.try_take("."):
            parts.append(p.take_word())
        assert all(map(is_word, parts))
        return cls(parts)


class Bracket(Generic[T]):
    item: T

    def __init__(self, item: T):
        self.item = item

    def __sql__(self):
        return f"({self.item})"

    @classmethod
    def __parse__(cls, p: ParseState):
        args = typing.get_args(cls)
        assert len(args) == 1
        p.take("(")
        x = p.parse(args[0])
        p.take(")")
        return cls(x)


class BracketList(Generic[T], list[T]):
    def __sql__(self):
        return f"({', '.join(map(sql, self))})"

    @classmethod
    def __parse__(cls, p: ParseState):
        T = typing.get_args(cls)[0]
        item: Bracket[list] = p.parse(Bracket[list[T]])
        return BracketList(item.item)


@sql.register(BracketList)
def _sql_blist(x: BracketList):
    return x.__sql__()


@sql_clause
class CommonTableExpr:
    table_name: str
    columns: O[BracketList[str]]
    _as: L["AS"]
    mat: O[L["MATERIALIZED", "NOT MATERIALIZED"]]
    select: Bracket["SelectStatement"]


@sql_clause
class WithRecClause:
    _kw: L["WITH"]
    rec: O[L["RECURSIVE"]]
    tables: list[CommonTableExpr]


Alias = tuple[L["AS"], str]


@sql_clause
class AliasedExpr:
    expr: Expr
    _as: O[L["AS"]] = None
    alias: O[Alias] = None


JoinOp = U[
    L[","],
    tuple[
        U[
            tuple[
                O[L["NATURAL"]],
                U[tuple[L["LEFT", "RIGHT", "FULL"], O[L["OUTER"]]], L["INNER"]],
            ],
            L["CROSS"],
        ],
        L["JOIN"],
    ],
]

JoinConstraint = None | tuple[L["ON"], Expr] | tuple[L["USING"], BracketList[str]]


@sql_clause
class JoinClause:
    table: "TableOrSubquery"
    joins: list[tuple[JoinOp, "TableOrSubquery", JoinConstraint]]


@sql_clause
class TableSelector:
    table_name: Identifier
    alias: None | Alias


@sql_clause
class TableFunctionApp:
    table_function_name: str
    args: BracketList[Expr]
    alias: None | Alias


@sql_clause
class NestedSelectStatement:
    smt: Bracket["SelectStatement"]
    alias: None | Alias


TableOrSubquery = U[
    TableSelector, TableFunctionApp, NestedSelectStatement, Bracket[JoinClause]
]


@sql_clause
class OrderingTerm:
    expr: Expr
    collate: None | tuple[L["COLLATE"], str] = None
    dir: None | L["ASC", "DESC"] = None
    nullage: None | L["NULLS FIRST", "NULLS LAST"] = None


@sql_clause
class WindowDefn:
    base_window_name: None | str
    partition_by_clause: None | tuple[L["PARTITION BY"], list[Expr]]
    order_by_clause: None | tuple[L["ORDER BY"], list[OrderingTerm]]
    frame_spec: None | str


@sql_clause
class WindowClause:
    _kw: L["WINDOW"]
    windows: list[tuple[str, L["AS"], Bracket["WindowDefn"]]]


@sql_clause
class FromClause:
    _kw: L["FROM"] = "FROM"
    items: JoinClause | list[TableOrSubquery]

    @classmethod
    def of_table_name(cls, table_name: str):
        ts = TableSelector(table_name=Identifier([table_name]), alias=None)
        return cls(items=[ts])


@sql_clause
class WhereClause:
    _where: L["WHERE"]
    expr: Expr

    def __and__(self, other):
        other = expr(other)
        return dataclasses.replace(self, expr=self.expr & other)

    @classmethod
    def create(cls, e):
        return cls(_where="WHERE", expr=expr(e))


@sql_clause
class SelectCore:
    _kw: L["SELECT"] = "SELECT"
    smode: O[L["DISTINCT", "ALL"]] = None
    columns: L["*"] | list[AliasedExpr]
    from_clause: None | FromClause = None
    where_clause: None | WhereClause = None
    group_by_clause: None | tuple[L["GROUP BY"], list[Expr]] = None
    having_clause: None | tuple[L["HAVING"], Expr] = None
    window_clause: None | WindowClause = None

    def add_where(self, e: Expr):
        e = expr(e)
        if self.where_clause is None:
            return dataclasses.replace(self, where_clause=WhereClause.create(e))
        else:
            return dataclasses.replace(self, where_clause=self.where_clause & expr)


class SelectValues:
    _kw: L["VALUES"]
    values: list[BracketList[Expr]]


@sql_clause
class LimitClause:
    _kw: L["LIMIT"] = "LIMIT"
    limit: Expr
    mode: O[tuple[L[",", "OFFSET"], Expr]] = None


CompoundOperator: TypeAlias = L["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]


@sql_clause
class OrderByClause:
    _kw: L["ORDER BY"] = "ORDER BY"
    terms: list[OrderingTerm]


@dataclasses.dataclass(kw_only=True)
class SelectStatement:
    with_rec: O[WithRecClause] = None
    core: list[SelectCore | SelectValues]
    compound_ops: list[CompoundOperator] = dataclasses.field(default_factory=list)
    order_by_clause: None | OrderByClause = None
    limit_clause: None | LimitClause = None

    @property
    def is_simple(self):
        """A select statement is simple if it has no compound operators."""
        return len(self.compound_ops) == 0

    def __sql__(self):
        acc = []
        if self.with_rec is not None:
            acc.append(sql(self.with_rec))
        cores = list(map(sql, self.core))
        compound_ops = list(map(sql, self.compound_ops))
        assert len(cores) == len(compound_ops) + 1
        acc += list(interlace(cores, compound_ops))
        if self.order_by_clause is not None:
            acc.append(sql(self.order_by_clause))
        if self.limit_clause is not None:
            acc.append(sql(self.limit_clause))
        return " ".join(acc)

    @classmethod
    def __parse__(cls, p: ParseState):
        with_rec = p.try_parse(WithRecClause)
        C = U[SelectCore, SelectValues]
        core = [p.parse(C)]
        compound_ops = []
        while True:
            op = p.try_parse(CompoundOperator)
            if op is None:
                break
            compound_ops.append(op)
            core.append(p.parse(C))
        order_by_clause = p.try_parse(OrderByClause)
        limit_clause = p.try_parse(LimitClause)
        return cls(
            with_rec=with_rec,
            core=core,
            compound_ops=compound_ops,
            order_by_clause=order_by_clause,
            limit_clause=limit_clause,
        )


@sql_clause
class ReturningClause:
    _kw: L["RETURNING"] = "RETURNING"
    items: L["*"] | list[AliasedExpr]


InsertOrClause = tuple[L["OR"], L["ABORT", "FAIL", "IGNORE", "REPLACE", "ROLLBACK"]]


@sql_clause
class Setter:
    col: Identifier | BracketList[Identifier]
    _eq: L["="] = "="
    value: Expr


Setters = list[Setter]


@sql_clause
class ConflictTarget:
    columns: BracketList[Identifier]
    where_clause: O[WhereClause]


@sql_clause
class ConflictUpdate:
    _kw: L["UPDATE SET"]
    setters: Setters
    where_clause: None | tuple[L["WHERE"], Expr]


@sql_clause
class UpsertClause:
    _kw: L["ON CONFLICT"] = "ON CONFLICT"
    target: O[ConflictTarget] = None
    _do: L["DO"] = "DO"
    update: L["NOTHING"] | ConflictUpdate = "NOTHING"


ON_CONFLICT_DO_NOTHING = UpsertClause()


@sql_clause
class InsertValues:
    _kw: L["VALUES"] = "VALUES"
    values: list[BracketList[Expr]]
    upsert: O[UpsertClause] = None


@sql_clause
class InsertSelect:
    smt: SelectStatement
    upsert: O[UpsertClause]


@sql_clause
class InsertStatement:
    with_rec: O[WithRecClause] = None
    kw: tuple[L["INSERT"], O[InsertOrClause]] | L["REPLACE"] = ("INSERT", None)
    _into: L["INTO"] = "INTO"
    table_name: Identifier
    table_alias: O[Alias] = None
    columns: O[BracketList[Identifier]] = None
    values: InsertValues | InsertSelect | L["DEFAULT VALUES"]
    returning_clause: None | ReturningClause = None


@sql_clause
class QualifiedTableName:
    name: Identifier
    alias: None | Alias = None
    index: None | tuple[L["INDEXED BY"], Identifier] | tuple[L["NOT INDEXED"]] = None


@sql_clause
class UpdateStatement:
    with_rec: O[WithRecClause] = None
    _update: L["UPDATE"] = "UPDATE"
    or_clause: O[InsertOrClause] = None
    name: QualifiedTableName
    _set: L["SET"] = "SET"
    setters: Setters
    from_clause: FromClause | None = None
    where_clause: None | WhereClause = None
    returning_clause: None | ReturningClause = None


@sql_clause
class DeleteStatement:
    with_rec: O[WithRecClause] = None
    _delete: L["DELETE FROM"] = "DELETE FROM"
    table_name: QualifiedTableName
    where_clause: None | WhereClause = None
    returning_clause: None | ReturningClause = None


@sql_clause
class TypeName:
    name: tuple[str, ...]
    number: O[Bracket[int | tuple[int, L[","], int]]]


# [todo] column constraints.
ConflictClause = tuple[
    L["ON CONFLICT"], L["ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"]
]


@sql_clause
class PrimaryKeyColumnConstraint:
    _kw: L["PRIMARY KEY"]
    ascdesc: O[L["ASC", "DESC"]]
    conflict_clause: O[ConflictClause]
    autoincrement: O[L["AUTOINCREMENT"]]


@sql_clause
class GeneratedConstraint:
    _ga: O[L["GENERATED ALWAYS"]]
    _as: L["AS"]
    value: Bracket[Expr]
    storage: O[L["STORED", "VIRTUAL"]]


@sql_clause
class FKAction:
    _on: L["ON"]
    updel: L["DELETE", "UPDATE"]
    action: L["SET NULL", "SET DEFAULT", "CASCADE", "RESTRICT", "NO ACTION"]


@sql_clause
class FKDefer:
    deferrable: L["DEFERRABLE", "NOT DEFERRABLE"]
    init: O[L["INITIALLY DEFERRED", "INITIALLY IMMEDIATE"]]


@sql_clause
class ForeignKeyClause:
    _ref: L["REFERENCES"]
    table_name: Identifier
    columns: O[BracketList[Identifier]]
    actions: O[list[tuple[L["MATCH"], str] | FKAction]]
    defer: O[FKDefer]


CheckConstraint = tuple[L["CHECK"], Bracket[Expr]]

ColumnConstraint = (
    PrimaryKeyColumnConstraint
    | tuple[L["NOT NULL", "UNIQUE"], O[ConflictClause]]
    | CheckConstraint
    | tuple[L["DEFAULT"], Bracket[Expr] | IntLiteral | QuotedString]
    | tuple[L["COLLATE"], str]
    | ForeignKeyClause
    | GeneratedConstraint
)


@sql_clause
class IndexedColumn:
    name: Identifier | Expr
    collate: O[tuple[L["COLLATE"], str]]
    asc: O[L["ASC", "DESC"]]


@sql_clause
class KeyTableConstraint:
    kw: L["PRIMARY KEY", "UNIQUE"]
    columns: BracketList[IndexedColumn]
    conflict_clause: O[ConflictClause]


TableConstraint = (
    KeyTableConstraint
    | CheckConstraint
    | tuple[L["FOREIGN KEY"], BracketList[Identifier], ForeignKeyClause]
)

TypeNameNumber: TypeAlias = O[Bracket[int | tuple[int, L[","], int]]]


@sql_clause
class ColumnDef:
    name: str
    type: tuple[str, ...] = ()
    number: TypeNameNumber = None
    constraints: tuple[ColumnConstraint, ...] = ()

    @classmethod
    def __parse__(cls, p: ParseState):
        name = p.parse(str)
        stopwords = [
            "PRIMARY",
            "NOT",
            "UNIQUE",
            "CHECK",
            "DEFAULT",
            "COLLATE",
            "REFERENCES",
            "GENERATED",
            "AS",
            ",",
        ]
        tns: list[str] = []
        while True:
            if p.can_take(*stopwords):
                break
            tns.append(p.parse(str))
        if len(tns) > 0:
            number = p.parse(TypeNameNumber)
        else:
            number = None
        constraints = p.parse(tuple[ColumnConstraint, ...])
        return cls(name=name, type=tuple(tns), number=number, constraints=constraints)


@sql_clause
class ColumnsDef:
    columns: list[ColumnDef]
    constraints: list[TableConstraint]

    def __sql__(self):
        l1 = ", ".join(map(sql, self.columns))
        if len(self.constraints) > 0:
            l2 = ", ".join(map(sql, self.constraints))
            return f"({l1}, {l2})"
        else:
            return f"({l1})"

    @classmethod
    def __parse__(cls, p: ParseState):
        # this needs a lookahead parser because you don't know whether you are
        # doing constraints until after comma.
        columns: list[ColumnDef] = []
        constraints: list[TableConstraint] = []
        tckw = ["CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN"]
        p.take("(")
        columns.append(p.parse(ColumnDef))
        while True:
            if p.try_take(")"):
                return cls(columns=columns, constraints=constraints)
            p.take(",")
            if p.can_take(*tckw):
                break
            columns.append(p.parse(ColumnDef))
        constraints.append(p.parse(TableConstraint))
        while True:
            if p.try_take(")"):
                break
            p.take(",")
            constraints.append(p.parse(TableConstraint))
        return cls(columns=columns, constraints=constraints)


@sql_clause
class CreateTableStatement:
    _create: L["CREATE"]
    temp: O[L["TEMP", "TEMPORARY"]]
    _table: L["TABLE"]
    if_not_exists: O[L["IF NOT EXISTS"]]
    table_name: Identifier
    columns: ColumnsDef
    options: O[list[L["WITHOUT ROWID", "STRICT"]]]

    # [todo] AS (select statement) branch


Statement: TypeAlias = (
    CreateTableStatement
    | InsertStatement
    | UpdateStatement
    | SelectStatement
    | DeleteStatement
    # DropTableStatement
)


def parse_statement(query: str, *args) -> Statement:
    p = ParseState(query)
    pc = PlaceholderContext(args)
    with newctx(ph_ctx, pc):
        s = p.parse(Statement)
        p.try_take(";")
        assert p.is_eof()
        return s


def parse_commands(query: str, *args, **kwargs):
    p = ParseState(query)
    pc = PlaceholderContext(args, kwargs)
    statements = []
    with newctx(ph_ctx, pc):
        statements.append(p.parse(Statement))
        while p.try_take(";"):
            if p.is_eof():
                break
            statements.append(p.parse(Statement))
    assert len(pc.args) == 0, f"Not all placeholders were bound: {pc.args}"
    return statements


"""
Cool things we can do:
- automatically write hypothesis.strategies for all the types
- write a type checker for the sql expressions.
- write a validator for whether all variables and placeholders are bound correctly.
- write a LLM-powered generator for the types
"""
