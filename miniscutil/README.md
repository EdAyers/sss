# miniscutil

[![PyPI - Version](https://img.shields.io/pypi/v/miniscutil.svg)](https://pypi.org/project/miniscutil)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/miniscutil.svg)](https://pypi.org/project/miniscutil)

-----

Collection of miscellaneous functions and methods that I wish were in core. I think that some of them are implemented by existing libraries.

This is supposed to be a big bag of code that I use in lots of my projects, eventually they should be replaced with an existing library or moved into their own specialized library.

- `dispatch.py` hijacks the dispatcher used by `functools.singledispatch` as its own class. This is used to implement a `classdispatch` decorator that can accept a type as argument.
- `adapt`, an implementation of [PEP-246](https://peps.python.org/pep-0246/#specification)
- `ofdict.py` converts to and from a json-like object `JsonLike = Union[str, int, float, bool, type(None), list[JsonLike], dict[str, JsonLike]]`. It overlaps a lot with `attrs`, `cattrs` and `pydantic` libraries.
- `deep.py` implements a deepcopy-like reduction system for traversing, mapping and serializing arbitrary python objects.
- `deepeq.py` implements a deep-equality algorithm.
- `current.py` is a base class for implementing the singleton pattern.
- `sum.py` discriminated sum type.
