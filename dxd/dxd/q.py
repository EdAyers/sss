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
    ClassVar,
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

T = TypeVar("T")


@functools.singledispatch
def sql(x: Any):
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
            "Unescaped space in string detected. Did you mean to use a string literal?"
        )
    return x  # escaping is done on string literals.


@sql.register(int)
def _sql_int(x: int):
    return str(x)


@dataclass_transform()
def sql_expr(cls: Type[T]) -> Type[T]:
    ParseState.type_registry[cls.__name__] = cls
    cls = dataclasses.dataclass(cls)
    fields = dataclasses.fields(cls)

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

    # def init(self, *args, **kwargs):
    #     n = len(args)
    #     assert n <= len(fields)
    #     for i in range(n):
    #         setattr(self, fields[i].name, args[i])
    #     rest = fields[n:]
    #     for field in rest:
    #         v = kwargs.pop(field.name, field.default)
    #         if v is dataclasses.MISSING:
    #             raise ValueError(f"missing required argument {field.name}")
    #         # [todo] assert right type
    #         setattr(self, field.name, v)
    #     assert len(kwargs) == 0
    # setattr(cls, "__init__", init)

    def __parse__(cls, p: ParseState):
        types = [f.type for f in dataclasses.fields(cls)]
        vs = p.parse(tuple[(*types,)])  # type: ignore
        return cls(*vs)

    def __sql__(self):
        acc = []
        for field in fields:
            v = getattr(self, field.name)
            if v is not None:
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


class QuotedString(str, Expr):
    def __sql__(self):
        # [todo] use the sqlite quoting function
        # doesn't seem to be possible without using placeholders and executing?
        return shlex.quote(self)

    @classmethod
    def __parse__(cls, p):
        item = p.next()
        qs = ['"', "'"]
        for q in qs:
            if item.startswith(q) and item.endswith(q):
                # [todo] unescaping?
                return item.strip(q)
        raise ValueError(f"Expected quoted string, got {item}")


class IntLiteral(int, Expr):
    def __sql__(self):
        return str(self)

    @classmethod
    def __parse__(cls, p):
        item = p.next()
        return int(item)


@dataclasses.dataclass
class InfixOp(Expr):
    left: Expr
    right: Expr
    precedence: int
    operator: str

    def __sql__(self):
        l = with_brackets(self.left, self.precedence)
        r = with_brackets(self.right, self.precedence)
        return f"{l} {self.operator} {r}"


@dataclasses.dataclass
class PrefixOp(Expr):
    right: Expr
    precedence: int
    operator: str

    def __sql__(self):
        r = with_brackets(self.right, self.precedence)
        return f"{self.operator} {r}"


PREFIX_OPS = {"NOT": 2, "~": 11, "+": 11, "-": 11}
BINARY_OPS = {
    "||": 9,
    "->": 9,
    "->>": 9,
    "*": 8,
    "/": 8,
    "%": 8,
    "+": 7,
    "-": 7,
    "&": 6,
    "|": 6,
    "<<": 6,
    ">>": 6,
    "<": 4,
    ">": 4,
    "<=": 4,
    ">=": 4,
    "=": 3,
    "==": 3,
    "!=": 3,
    "<>": 3,
    "IS": 3,
    "IS NOT": 3,
    "IS DISTINCT FROM": 3,
    "IS NOT DISTINCT FROM": 3,
    "AND": 1,
    "OR": 0,
}

POSTFIX_OPS = {
    "ISNULL": 3,
    "NOTNULL": 3,
    "NOT NULL": 3,
}


@dataclasses.dataclass
class PostfixOp(Expr):
    left: Expr
    precedence: int
    operator: str

    def __sql__(self):
        l = with_brackets(self.left, self.precedence)
        return f"{l} {self.operator}"


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
    for prefix_op, prec in PREFIX_OPS.items():
        if p.try_take(prefix_op):
            right = parse_expr(p, prec)
            return PrefixOp(right, prec, prefix_op)
    # [todo] function name
    #
    item = parse_atom(p)
    return parse_op(p, item, l_prec)


Globbie = tuple[O[L["NOT"]], L["GLOB", "REGEXP", "MATCH", "LIKE"]]


def parse_op(p: ParseState, left, l_prec: int) -> Expr:
    if p.can_take(")"):
        return left
    for binary_op, prec in BINARY_OPS.items():
        if l_prec > prec:
            continue
        if p.try_take(binary_op):
            right = parse_expr(p, prec)
            return InfixOp(left, right, prec, binary_op)
    for postfix_op, prec in POSTFIX_OPS.items():
        if l_prec > prec:
            continue
        if p.try_take(postfix_op):
            return PostfixOp(left, prec, postfix_op)
    if p.try_take("COLLATE"):
        raise NotImplementedError()
    if p.try_parse(Globbie):
        raise NotImplementedError()
    if p.try_parse(tuple[O[L["NOT"]], L["BETWEEN", "IN"]]):
        raise NotImplementedError()
    return left


@dataclasses.dataclass
class Identifier(Expr):
    parts: list[str]

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

    def __sql__(self):
        return f"({self.item})"

    @classmethod
    def __parse__(cls, p: ParseState):
        args = typing.get_args(cls)
        assert len(args) == 1
        p.take("(")
        x = p.parse(args[0])
        p.take(")")
        return x


class BracketList(Generic[T], list[T]):
    def __sql__(self):
        return f"({', '.join(map(str, self))})"

    @classmethod
    def __parse__(cls, p: ParseState):
        return Bracket[list[T]].__parse__(p)


sql.register(BracketList)(BracketList.__sql__)


@sql_expr
class CommonTableExpr:
    table_name: str
    columns: O[BracketList[str]]
    _as: L["AS"]
    mat: O[L["MATERIALIZED", "NOT MATERIALIZED"]]
    select: Bracket["SelectStatement"]


CompoundOperator = L["UNION", "UNION ALL", "INTERSECT", "EXCEPT"]


@sql_expr
class WithRecClause:
    _kw: L["WITH"]
    rec: O[L["RECURSIVE"]]
    tables: list[CommonTableExpr]


Alias = tuple[L["AS"], str]

ResultColumn = U[
    tuple[Expr, O[Alias]],
    L["*"],
]

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


@sql_expr
class JoinClause:
    table: "TableOrSubquery"
    joins: list[tuple[JoinOp, "TableOrSubquery", JoinConstraint]]


@sql_expr
class TableSelector:
    table_name: Identifier
    alias: None | Alias


@sql_expr
class TableFunctionApp:
    table_function_name: str
    args: BracketList[Expr]
    alias: None | Alias


@sql_expr
class NestedSelectStatement:
    smt: Bracket["SelectStatement"]
    alias: None | Alias


TableOrSubquery = U[
    TableSelector, TableFunctionApp, NestedSelectStatement, Bracket[JoinClause]
]


@sql_expr
class OrderingTerm:
    _kw: L["ORDER BY"]
    expr: Expr
    collate: None | tuple[L["COLLATE"], str]
    dir: None | L["ASC", "DESC"]
    nullage: None | L["NULLS FIRST", "NULLS LAST"]


@sql_expr
class WindowDefn:
    base_window_name: None | str
    partition_by_clause: None | tuple[L["PARTITION BY"], list[Expr]]
    order_by_clause: None | tuple[L["ORDER BY"], list[OrderingTerm]]
    frame_spec: None | str


@sql_expr
class WindowClause:
    _kw: L["WINDOW"]
    windows: list[tuple[str, L["AS"], Bracket["WindowDefn"]]]


@sql_expr
class SelectCore:
    _kw: L["SELECT"]
    smode: O[L["DISTINCT", "ALL"]]
    columns: list[ResultColumn]
    from_clause: None | tuple[L["FROM"], JoinClause | list[TableOrSubquery]]
    where_clause: None | tuple[L["WHERE"], Expr]
    group_by_clause: None | tuple[L["GROUP BY"], list[Expr]]
    having_clause: None | tuple[L["HAVING"], Expr]
    window_clause: None | WindowClause


class SelectValues:
    _kw: L["VALUES"]
    values: list[BracketList[Expr]]


@sql_expr
class LimitClause:
    _kw: L["LIMIT"]
    limit: Expr
    mode: O[tuple[L[",", "OFFSET"], Expr]]


@sql_expr
class SelectStatement:
    with_rec: O[WithRecClause]
    core: SelectCore | SelectValues
    order_by_clause: None | list[Expr]
    limit_clause: None | LimitClause
    _semicolon: O[L[";"]]


def parse_statement(p: ParseState):
    s = p.parse(SelectStatement)
    p.try_take(";")
    return s


if __name__ == "__main__":
    ps = ParseState("SELECT * FROM foo.cheese WHERE x = 3 + 2;")
    x = parse_statement(ps)
    print(x)
    print(sql(x))
