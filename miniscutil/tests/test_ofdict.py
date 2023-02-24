from dataclasses import dataclass, asdict
from typing import List
from miniscutil.deepeq import deepeq
from miniscutil.ofdict import (
    ofdict,
    todict,
    todict_rec,
    MyJsonEncoder,
    TypedJsonDecoder,
)
import hypothesis.strategies as st


@dataclass
class Bar:
    cheese: int
    toast: str


@dataclass
class Foo:
    bar: Bar
    bar2: Bar
    blap: List[Bar]


examples = [
    (Foo, Foo(Bar(2, "hello"), Bar(3, "world"), blap=[Bar(4, "whiz"), Bar(5, "pop")])),
]


def todict_roundtrip1(T, o):
    j = todict_rec(o)
    y = ofdict(T, j)
    assert deepeq(o, y)


def todict_roundtrip2(T, o):
    j = MyJsonEncoder().encode(o)
    y = TypedJsonDecoder(T).decode(j)
    assert deepeq(o, y)


def test_foo():
    for T, x in examples:
        todict_roundtrip1(T, x)
        todict_roundtrip2(T, x)
