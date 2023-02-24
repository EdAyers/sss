from enum import Enum
from typing import get_args, get_origin
from miniscutil import classdispatch


@classdispatch
def xxx(X):
    return "base"


@xxx.register(list)
def _list_xxx(T):
    o = get_origin(T)
    if o is None:
        return "list"
    assert o is list
    t = get_args(T)
    assert len(t) == 1
    return f"list[{t[0].__name__}]"


@xxx.register(Enum)
def _enum_xxx(T):
    assert issubclass(T, Enum)
    return f"enum[{T.__name__}]"


class MyEnum(Enum):
    a = "A"
    b = "B"


def test_classdispatch():
    assert xxx(str) == "base"

    assert xxx(list) == "list"
    assert xxx(list[int]) == "list[int]"

    assert xxx(MyEnum) == "enum[MyEnum]"
