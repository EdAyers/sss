from hitsave import memo
import numpy as np
import logging
from hitsave.local.decorator import SavedFunction
from hitsave.local.inspection import HashingPickler
import pprint


@memo
def f(x):
    # try changing the method body and witness the cache invalidating!
    return x + x + x


@memo
def g(y):
    return f(y) + y + y


@memo
def gg(y):
    return np.ones(2**y)


@memo()
def gggg(x: int):
    return 4 * x


@memo(local_only=True)
def ggg(y, x: int):
    return y + x + y


def test_fns_are_right_type():
    for f in [g, gg, gggg, ggg]:
        assert isinstance(f, SavedFunction)


@memo
def giant_list():
    return [i for i in range(100000)]


def test_giantlist():
    x = giant_list()


def test_savesave():
    # [todo] view logs
    print(g(4))
    print(g(4))
    print(f(3))
    print(g(5))


def test_biggies():
    for x in range(20):
        print(len(gg(x)))


def test_hashing_pickler_deps():
    o = {
        "ident": lambda x: x,
        "k": lambda x, y: x,
        "other": test_biggies,
        "g": range,
    }
    hp = HashingPickler()
    hp.dump(o)
    ds = hp.code_dependencies
    # pprint.pp(ds)
    # [note] the two lambdas are dropped as code dependencies and are just hashed as source strings.
    # [todo] future versions of HashingPickler will distinguish these.
    assert len(ds) == 2


MY_LIST = [1, 2, 3, 4]


@memo
def double_mylist():
    return [x * 2 for x in MY_LIST]


def test_closure2():
    global MY_LIST
    assert double_mylist() == [2, 4, 6, 8]
    MY_LIST = [10, 11]
    assert double_mylist() == [20, 22]


if __name__ == "__main__":
    test_hashing_pickler_deps()

    test_savesave()

    test_giantlist()

    test_biggies()
