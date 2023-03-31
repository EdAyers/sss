from decimal import Decimal
from fractions import Fraction
from numbers import Number
import sys
from typing import Any, List, Optional, Type
from hypothesis import given, assume
import pytest
import json
import pprint
import itertools
import math
import cmath
import numpy as np
import hypothesis.extra.numpy as hsnp
import hypothesis.strategies as hs
import datetime
from hitsave.common import Symbol

""" Strategies for hitsave. The idea is that this file contains

every kind of object, and hypothesis strategies for producing every kind of object,
that we consider to be supported by hitsave.

That means that all of these objects should be hashable, picklable and unpicklable with our system.


We define the following terms:
- A python object is __composite__ when its data contains references to other python objects.
- A python object is __atomic__ when it is not composite. Strings, ints, bools, and Numpy arrays are atomic.
- A python object is __builtin__ when you could make it without defining your own types or importing stuff.
- All objects have a type. A __type__ is an object which subtypes `type`.

## [todo]

- Include a set of custom-made types:
    - dataclasses
    - enums
    - classes with a state dict

- Include pytorch datatypes.
- numpy datatypes

- Include pathological types that shouldn't really make it in to a hitsave record.
  These should throw things.
    - logging.Logger.
 """

supported_types: List[Any] = [
    type,
    object,
    # primitive
    bytes,
    dict,
    # composite
    list,
    bool,
    tuple,
    set,
    # numbers
    int,
    float,
    complex,
    Fraction,
    Decimal,
    # datteime
    datetime.datetime,
    datetime.date,
    datetime.time,
    datetime.tzinfo,
    datetime.timedelta,
    datetime.timezone,
    # numpy
    # np.dtype,
    # np.ndarray,
]


def types() -> hs.SearchStrategy[Type]:
    return hs.sampled_from(supported_types)


def numbers(allow_nan=True) -> hs.SearchStrategy[Number]:
    return hs.one_of(
        [
            hs.floats(allow_nan=allow_nan),
            hs.decimals(allow_nan=allow_nan),
            hs.complex_numbers(allow_nan=allow_nan),
            hs.fractions(),
            hs.integers(),
        ]
    )


def atoms(allow_nan=True) -> hs.SearchStrategy[Any]:
    """An atomic python object is one whose data does not refer to any other python objects.

    There is an allow_nan flag, because nans are not reflexive and this is really annoying.
    """
    return hs.one_of(
        [
            numbers(allow_nan=allow_nan),
            hs.datetimes(),
            hs.dates(),
            hs.times(),
            hs.binary(),
            hs.booleans(),
            hs.from_type(str),
            hs.just(None),
            hs.just(...),
            types(),
            # pytorch tensors [todo]
            # numpy arrays
            # hs.from_type(np.dtype),
            # hs.from_type(np.ndarray),
            # pandas dataframes
        ]
    )


def hashables(allow_nan=True):
    return hs.one_of(
        [
            hs.booleans(),
            hs.just(None),
            hs.from_type(str),
            numbers(allow_nan=False),
        ]
    )


def objects():
    """Generates all python objects that hitsave supports."""
    return hs.one_of(
        [
            hs.sets(hashables()),
            hs.recursive(
                atoms(),
                lambda x: hs.one_of(
                    [
                        hs.dictionaries(hashables(), x),
                        hs.lists(x),
                    ]
                ),
            ),
        ]
    )


def module_names() -> hs.SearchStrategy[str]:
    ks = list(sys.modules.keys())
    return hs.sampled_from(ks)


def symbols_in_module(module_name: str) -> hs.SearchStrategy[Symbol]:
    m = sys.modules.get(module_name)
    ks: list[Any] = [k for k in dir(m) if hasattr(m, k)]
    ks.append(None)
    # [todo] include subnamespaces (eg class methods)
    return hs.sampled_from(ks).map(lambda decl_name: Symbol(module_name, decl_name))


def symbols() -> hs.SearchStrategy[Symbol]:
    return module_names().flatmap(symbols_in_module)


hs.register_type_strategy(Symbol, lambda _: symbols())
