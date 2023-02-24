from decimal import Decimal
from functools import singledispatch
import math
import cmath
from .deep import reduce
import numpy as np

"""
Deep equality checking.

This is used by the test library.

[todo] there must be a library that already does this.

"""


def children_eq(eq_rec):
    def inner(a1, a2):
        if a1 is a2:
            return True
        if type(a1) != type(a2):
            return False
        rv1, rv2 = map(reduce, (a1, a2))
        if rv1 is None and rv2 is None:
            return a1 == a2
        if rv1 is None:
            return False
        if rv2 is None:
            return False
        if len(rv1) != len(rv2):
            return False
        for ((p1, k1), v1), ((p2, k2), v2) in zip(rv1, rv2):
            if p1 != p2:
                return False
            if not deepeq(k1, k2):
                return False
            if not deepeq(v1, v2):
                return False
        return True

    return inner


@singledispatch
def deepeq(a1, a2):
    """Checks if the two items are structurally equal.

    Different types implies not equal.
    For floating point numbers, nan is reflexive
    (note usually `nan != nan`).
    """
    if type(a1) != type(a2):
        return False
    if a1 == a2:
        return True
    return children_eq(deepeq)(a1, a2)


deepeq.register(list, children_eq(deepeq))
deepeq.register(dict, children_eq(deepeq))


@deepeq.register(np.ndarray)
def _np_eq(x, y):
    if x.shape != y.shape:
        return False
    if x.size == 0:
        return True
    return x == y


@deepeq.register(float)
def _float_deepeq(x, y):
    if type(x) != type(y):
        return False
    if math.isnan(x) and math.isnan(y):
        return True
    return x == y


@deepeq.register(Decimal)
def _decimal_deepeq(x, y):
    if type(x) != type(y):
        return False
    if x.is_snan():
        return y.is_snan()
    if y.is_snan():
        return False
    if x.is_nan() and y.is_nan():
        return True
    return x == y


@deepeq.register(complex)
def _complex_deepeq(x, y):
    if type(x) != type(y):
        return False
    return deepeq(x.real, y.real) and deepeq(x.imag, y.imag)
