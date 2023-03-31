from copy import deepcopy
from decimal import Decimal
import numbers
import sys
from hypothesis import given, assume
import pytest
import json
import pprint
from hitsave.local.inspection import (
    debug_value_digest,
    print_digest_diff,
    value_digest,
    HashingPickler,
)
import itertools
import math
import cmath
import hypothesis.strategies as hs
import datetime
import numpy as np
import os
import sys


from miniscutil.deep import reduce, reconstruct, traverse
from .strat import atoms, objects
from miniscutil.deepeq import deepeq

examples = [
    "hello",
    "",
    "0",
    0,
    1,
    -1,
    10 ^ 100,
    10 ^ 100 + 1,
    0.5,
    sys.float_info.epsilon,
    sys.float_info.max,
    sys.float_info.min,
    float("nan"),
    float("inf"),
    -float("inf"),
    [],
    [0, 1, 2],
    (0,),
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


def test_deephash_snapshot(snapshot):
    hs = [(repr(x), value_digest(x)) for x in examples]
    snapshot.assert_match(pprint.pformat(hs), "example_hashes.txt")
    hset = set(x[1] for x in hs)
    assert len(hset) == len(hs), "Hash collision detected"


def test_hash_deterministic():
    for x in examples:
        a = value_digest(x)
        b = value_digest(x)
        assert a == b


def prop__hash_invariant_on_copy(x):
    a = value_digest(x)
    xx = deepcopy(x)
    b = value_digest(xx)
    if a != b:
        print(debug_value_digest(x)[1])
        print(debug_value_digest(xx)[1])
        print_digest_diff(x, xx)
    assert a == b, f"{repr(x)} != {repr(xx)}"


def test_hash_invariant_on_copy():
    for x in examples:
        prop__hash_invariant_on_copy(x)


def test_noconflicts():
    hs = [(x, value_digest(x)) for x in examples]
    for (x1, h1), (x2, h2) in itertools.combinations(hs, 2):
        assert h1 != h2, f"{x1} and {x2} produced same hash {h1}."


@given(atoms(), atoms())
def test_atoms_prop(a1, a2):
    assume(not deepeq(a1, a2))
    d1, d2 = map(value_digest, (a1, a2))
    assert d1 != d2


def test_value_digest_ok_lambda():
    q = lambda x: x + 2
    p = q
    q = lambda x: x + 2
    # currently, this should fallback to
    qd = value_digest(q)
    pd = value_digest(p)
    if qd != pd:
        print_digest_diff(q, p)
    assert qd == pd
