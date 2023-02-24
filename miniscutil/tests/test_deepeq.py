from copy import deepcopy
from decimal import Decimal
import sys
from miniscutil.deep import reduce, reconstruct, traverse
import itertools
import datetime
from miniscutil.deepeq import deepeq
import numpy as np

""" Testing for the test.deep and test.deepeq modules. """

examples = [
    "hello",
    "",
    "0",
    b"",
    b"0",
    0,
    1,
    -1,
    10 ^ 100,
    10 ^ 100 + 1,
    0.5,
    True,
    False,
    None,
    sys.float_info.epsilon,
    sys.float_info.max,
    sys.float_info.min,
    float("nan"),
    float("inf"),
    -float("inf"),
    [],
    [0, 1, 2],
    (0,),
    (),
    ["0"],
    {},
    {"x": 4, "y": 4},
    {"x": 4, "y": {"z": 4}},
    set(),
    set([1, 2, 3]),
    set([0]),
    Decimal("NaN"),
    Decimal("sNaN"),
    {datetime.date(2000, 1, 1): {}, 0: {}},
    {7.0, Decimal("Infinity")},
    # primitive types
    int,
    float,
    list,
    str,
    bytes,
    # numpy
    np.int32,
    np.array([]),
    np.array([[]]),
    np.array(4),
    np.zeros((0, 4)),
    np.zeros((0, 5)),
]


def test_examples_traverse():
    for x in examples:
        y = traverse(x)
        assert deepeq(y, x), f"{repr(x)} ≠ {repr(y)}"


def test_eg_deepeq():
    for x1 in examples:
        assert deepeq(x1, x1)
    for x1, x2 in itertools.combinations(examples, 2):
        assert not deepeq(x1, x2)


def test_deepeq_date():
    d1 = datetime.date(2000, 1, 1)
    d2 = datetime.date(2000, 1, 2)
    assert not deepeq(d1, d2)


def test_traverse_ld():
    a = [Decimal("sNaN")]
    b = traverse(a)
    assert deepeq(a, b)
