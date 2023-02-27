from abc import ABC
from dataclasses import Field
import logging
import selectors
from typing import Any, Callable, Generic, Optional, Type, TypeVar, Union, overload
from miniscutil.adapt import adapt, restore

from miniscutil.ofdict import MyJsonEncoder, TypedJsonDecoder
from .expr import Expr, AbstractExpr
from .column import Column

logger = logging.getLogger("dxd")

S = TypeVar("S")
R = TypeVar("R")


class Pattern(Generic[S]):
    """A list of Exprs and a function sending these exprs to a python value."""

    items: list[Expr]
    outfn: Callable[[list[Any]], S]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({repr(self.items), repr(self.outfn)})"

    def __len__(self):
        return len(self.items)

    @overload
    def __init__(self, obj: S):
        ...

    @overload
    def __init__(self, items: list["Expr"], outfn: Callable[[list[Any]], S]):
        ...

    @overload
    def __init__(self, obj: "AbstractExpr"):
        ...

    def __init__(self, obj: S, outfn=None):  # type: ignore
        if isinstance(obj, list) and isinstance(outfn, Callable):
            assert all(isinstance(x, Expr) for x in obj)
            self.items = obj
            self.outfn = outfn
            return
        elif isinstance(obj, Pattern):
            self.items = obj.items
            self.outfn = obj.outfn
            return
        elif isinstance(obj, Column):
            self.items = [Expr(obj)]
            self.outfn = lambda x: obj.restore(x[0])  # type: ignore
        elif isinstance(obj, (tuple, list)):
            self.items = []
            js = [0]
            j = 0
            ps = []
            for v in obj:
                p = Pattern(v)
                j += len(p)
                js.append(j)
                ps.append(p)
                self.items.extend(p.items)

            def blam(x) -> Any:
                acc = []
                for p, i, j in zip(ps, js[:-1], js[1:]):
                    assert j - i == len(p)
                    s = p.outfn(x[i:j])
                    acc.append(s)
                return type(obj)(acc)

            self.outfn = blam
        elif isinstance(obj, dict):
            self.items = []
            j = 0
            keys = list(obj.keys())
            jps = {}
            ps = []
            for k in keys:
                v = obj[k]
                p = Pattern(v)
                jps[k] = (j, p)
                j += len(p)
                self.items.extend(p.items)

            def blam(x) -> Any:
                acc = {}
                for k in keys:
                    (j, p) = jps[k]
                    j2 = j + len(p)
                    s = p.outfn(x[j:j2])
                    acc[k] = s
                return acc

            self.outfn = blam

        else:
            raise ValueError("bad pattern")

    def map(self, fn: Callable[[S], R]) -> "Pattern[R]":
        def comp(x):
            return fn(self.outfn(x))

        return Pattern(self.items, comp)

    def to_expr(self) -> "Expr":
        return Expr.binary(", ", self.items)


def sum(inner) -> Any:
    """Runs the SUM reduction on the given select pattern."""
    inner = Pattern(inner)
    if len(inner.items) != 1:
        raise ValueError(
            f"Pattern {inner} has {len(inner.items)} items but needs 1 for a SUM."
        )
    item = inner.items[0]
    return Pattern([Expr("SUM(?)", [item])], lambda x: x[0] or 0)
