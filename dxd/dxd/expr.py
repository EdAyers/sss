from abc import ABC, abstractmethod
from dataclasses import Field
import logging
import selectors
from typing import Any, Callable, Generic, Optional, Type, TypeVar, Union, overload
from miniscutil.adapt import adapt, restore
from miniscutil.ofdict import MyJsonEncoder, TypedJsonDecoder

logger = logging.getLogger("dxd")

S = TypeVar("S")
R = TypeVar("R")


class AbstractExpr(ABC):
    template: str
    values: list[Any]
    precedence: int = 0

    def __str__(self):
        [x, *xs] = self.template.split("?")
        acc = x
        for v, x in zip(self.values, xs):
            acc += f"⟨{str(v)}⟩"
            acc += x
        return acc

    def __add__(self, other):
        return Expr.binary("+", [self, other], 8)

    def __radd__(self, other):
        return Expr.binary("+", [other, self], 8)

    def __and__(self, other):
        return Expr.binary(" AND ", [self, other], precedence=2)

    def __or__(self, other):
        return Expr.binary(" OR ", [self, other], precedence=2)

    def __eq__(self, other):
        return Expr.binary(" = ", [self, other], 4)

    def __ne__(self, other):
        return Expr.binary(" != ", [self, other], 4)

    def __not__(self):
        return Expr("NOT ( ? )", [self], 3)

    def __bool__(self):
        raise ValueError(f"{self} is not a boolean expression")


class Expr(AbstractExpr):
    """A sqlite expression. That is, it's a template string full of '?'s and a value for each '?'.

    Any user-provided data should be represented as a '?' with an item in `values` to prevent injection attacks.
    However we don't add any checks for this.
    """

    template: str
    values: list[Any]
    precedence: int

    @classmethod
    def const(cls, template: str):
        return Expr(template, [])

    @classmethod
    def empty(cls):
        return Expr("", [])

    @overload
    def __init__(self, obj: "AbstractExpr"):
        """Creates a new Expr with the same values as the given one."""
        ...

    @overload
    def __init__(self, obj: str, values: list, precedence: Optional[int] = None):
        """Create an expression from the given '?'-bearing template string, where each '?' is replaced with the python value given in ``values``."""
        ...

    @overload
    def __init__(self, obj: Union[str, int, bytes]):
        """Create a new constant expression with the given value."""
        ...

    def __init__(
        self, obj, values: Optional[list] = None, precedence: Optional[int] = None
    ):
        if isinstance(obj, AbstractExpr):
            assert values is None
            self.template = obj.template
            self.values = obj.values
            self.precedence = obj.precedence
        elif isinstance(obj, str) and values is not None:
            [head, *tail] = obj.split("?")
            assert len(tail) == len(values)
            self.template = head
            self.values = []
            self.precedence = 0 if precedence is None else precedence
            for c, p in zip(map(Expr, values), tail):
                if c.precedence < self.precedence:
                    c = Expr.parens(c)
                self.template += c.template
                self.values.extend(c.values)
                self.template += p
        elif values is None:
            self.template = " ? "
            self.values = [obj]
            self.precedence = 100
        else:
            raise ValueError(f"Don't know how to make expression from {repr(obj)}.")

    def __repr__(self):
        return f"Expr({repr(self.template)}, {repr(self.values)}, precedence={self.precedence})"

    @classmethod
    def parens(cls, expr: "Expr") -> "Expr":
        return Expr("(?)", [expr], precedence=200)

    @classmethod
    def binary(cls, op: str, args: list[Any], precedence: int = 0):
        return Expr(op.join(" ? " for _ in args), args, precedence)

    # [todo] all the other operators.
    # [todo] rather than storing as strings; store as AST

    def append(self, *values) -> "Expr":
        return Expr.binary(" ", [self, *values], 0)
