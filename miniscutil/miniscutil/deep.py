"""

This file contains a variant of the reduce/reconstruct scheme used by pickle and deepcopy.
This is used to implement:
- a custom pickler.
- deep-equal helper
- deephash


Recap: Python's datamodel
-------------------------

Everything in Python's runtime is an **object**.
For every object ``o``, we have the following:
- **id** ``id(o)`` is an integer identity for the object. This is a kind of pointer or reference. When you do `a is b`, it is checking whether the identies are the same.
- **type** ``type(o)`` is another object representing the type.

Additionally, every object has some raw data.
For python classes, this data takes the form of an attribute dictionary.
For primitive types like ``int`` and ``bytes`` the data is self-explanatory.
This data may reference other python objects.
Call objects that reference other objects **composite objects** and ones which don't **atomic objects**.

Recap: deepcopy
---------------

Python's standard library comes with a function called `deepcopy`.
This creates a deep copy of the object by traversing composite objects and making copies of everything.
The ``pickle`` library uses the same mechanism.

It works by keeping a dispatch table mapping types to functions called **reductors** (aka pickle-functions, aka reduction-functions).
The full standard signature for a reductor is rather involved and can be found `here <https://docs.python.org/3/library/pickle.html#object.__reduce__>`_.
The main gist is that a reductor ``r`` for ``T`` maps ``t : T`` to a pair ``(ctor, ctor_args)``.
``ctor`` is a callable that takes ``ctor_args`` as arguments and returns an instance of ``T`` that is a shallow-copy of the original ``t``.
Usually ``ctor`` is the class ``T`` itself.
The reductor can return extra tuple-items to represent state, iterables, dictionaries.
To register a new reductor you can add it to the dispatch table using the `copyreg module <https://docs.python.org/3/library/copyreg.html>`_
or implement `__reduce__` or `__reduce_ex__` methods on your class.

References:
- https://github.com/python/cpython/blob/3.10/Lib/copy.py
- https://github.com/python/cpython/blob/3.10/Lib/copyreg.py
- https://docs.python.org/3/library/pickle.html#object.__reduce__
- https://docs.python.org/3/reference/datamodel.html#objects-values-and-types
- Out of band pickling https://peps.python.org/pep-0574/

Related work:
- https://github.com/Suor/funcy


- [todo] namedtuple
- [todo] set
- [todo] frozenset
- [todo] range, slice

"""
from collections import ChainMap
import copyreg
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Literal,
    Tuple,
    Type,
    Optional,
    Union,
)
from functools import wraps
from pickle import DEFAULT_PROTOCOL
from dataclasses import dataclass, field, fields, is_dataclass, replace


def _uses_default_reductor(cls):
    """Returns true when the given class does not override the default `__reduce__` function."""
    return (getattr(cls, "__reduce_ex__", None) == object.__reduce_ex__) and (
        getattr(cls, "__reduce__", None) == object.__reduce__
    )


reducer_dispatch_table = {}
""" Dispatch table for reducers. """
dispatch = ChainMap(reducer_dispatch_table, copyreg.dispatch_table)


def register_reducer(type):
    global reducer_dispatch_table

    def reg(func):
        reducer_dispatch_table[type] = func
        return func

    return reg


@register_reducer(list)
def list_reductor(l: list) -> Tuple:
    return (list, (), None, l)


@register_reducer(dict)
def dict_reductor(d: dict) -> Tuple:
    return (dict, (), None, None, d)


@register_reducer(tuple)
def tuple_reductor(t: tuple) -> Tuple:
    return (tuple, (), None, t)


def _sortkey(x):
    # [note]: can't use hash because it is not stable.
    return (type(x).__name__, repr(x))


opaque = set(
    [
        type(None),
        type(Ellipsis),
        type(NotImplemented),
        int,
        float,
        bool,
        complex,
        bytes,
        str,
        type,
    ]
)
""" Set of scalar values that can't be reduced. """


def register_opaque(type):
    """Register a type as not being reducible."""
    global opaque
    opaque.add(type)


@dataclass
class ReductionValue:
    """Output of __reduce__.

    Slightly deviate from the spec in that listiter and dictiter can be
    sequences and dicts (rather than just being iterables).
    The values are the same as the values in the tuple specified in the below
    reference: https://docs.python.org/3/library/pickle.html#object.__reduce__

    [todo] support the slots item.

    """

    func: Callable
    args: Tuple
    state: Optional[Union[dict, Tuple]] = field(default=None)
    listiter: Optional[list] = field(default=None)
    dictiter: Optional[dict] = field(default=None)

    def __post_init__(self):
        if self.listiter is not None and type(self.listiter) != list:
            self.listiter = list(self.listiter)
        if self.dictiter is not None and type(self.dictiter) != dict:
            self.dictiter = dict(self.dictiter)

    def map(self, f: Callable[[Any], Any]) -> "ReductionValue":
        return self.walk(lambda v, k: f(v))

    def walk(self, f: Callable[[Any, Any], Any]) -> "ReductionValue":
        """Apply f(v, k) to each child object `v` with index or key `k`."""
        args = tuple(f(v, i) for i, v in enumerate(self.args))
        if self.state:
            if isinstance(self.state, dict):
                state = {k: f(v, k) for k, v in self.state.items()}
            else:
                state = f(self.state, None)
        else:
            state = self.state
        return replace(
            self,
            args=args,
            state=state,
            listiter=self.listiter and [f(v, i) for i, v in enumerate(self.listiter)],
            dictiter=self.dictiter and {k: f(v, k) for k, v in self.dictiter.items()},
        )

    def __len__(self):
        """Number of children."""
        l = 0
        l += len(self.args)
        if self.state:
            l += len(self.state)
        if self.listiter:
            l += len(self.listiter)
        if self.dictiter:
            l += len(self.dictiter)
        return l

    def __iter__(self) -> Iterator[Tuple[Tuple[str, Any], Any]]:
        """Iterates on all of the child objects of the reduced value.

        Returns ((loc, key), value):
            loc: one of "listiter" | "dictiter" | "state" | "args"
            key: the index, attr name or dict-key of the child object.
            value: the value of the child object.
        """
        for i, arg in enumerate(self.args):
            yield (("args", i), arg)
        if self.state:
            if isinstance(self.state, dict):
                for k in sorted(self.state.keys(), key=_sortkey):
                    yield (("state", k), self.state[k])
            else:
                yield (("state", None), self.state)
        if self.listiter:
            for i, item in enumerate(self.listiter):
                yield (("listiter", i), item)
        if self.dictiter:
            for k in sorted(self.dictiter.keys(), key=_sortkey):
                yield (("dictiter", k), self.dictiter[k])

    """ Gets the type that this reduction will create when reconstructed. """

    @property
    def type(self) -> Type:
        if self.func.__name__ == "__newobj__":
            return self.args[0]
        elif isinstance(self.func, type):
            return self.func
        else:
            raise NotImplementedError(f"cannot get the class from {self}")


def reduce(obj) -> Optional[ReductionValue]:
    """Similar to `__reduce__()`.
    If `None` is returned, that means that reduce treats the given object as _opaque_.
    This means that it won't bother unfolding it any further.
    """

    def core(obj) -> Any:
        global dispatch
        if isinstance(obj, type):
            # all types are opaque.
            return None
        cls = type(obj)
        if cls in dispatch:
            reductor = dispatch.get(cls)
            if reductor is not None:
                return reductor(obj)
        if cls in opaque:
            return None
        if _uses_default_reductor(cls) and is_dataclass(cls):
            # Custom reduction for dataclasses that's a bit nicer.
            # It's not technically correct because fields are mutable and so
            # should be states.
            # [todo] support dataclasses with hidden state?
            return (cls, tuple(getattr(obj, f.name) for f in fields(obj)))
        reductor = getattr(obj, "__reduce_ex__", None)
        if reductor is not None:
            return reductor(DEFAULT_PROTOCOL)
        reductor = getattr(obj, "__reduce__", None)
        if reductor is not None:
            return reductor()
        raise TypeError(f"cannot reduce a {cls}.")

    rv = core(obj)
    if type(rv) == tuple:
        return ReductionValue(*rv)
    elif type(rv) == str:
        # strings are globals
        raise NotImplementedError(
            f"not sure how to make reduction value from string '{rv}'."
        )
    else:
        return rv


"""
Based on: https://github.com/python/cpython/blob/442674e37eb84f9da5701412f8ad94e4eb2774fd/Lib/copy.py#L259
"""


def reconstruct(rv: ReductionValue):
    # short circuits
    if rv.func == list and rv.listiter is not None:
        return list(rv.listiter)
    elif rv.func == tuple and rv.listiter is not None:
        return tuple(rv.listiter)
    elif rv.func == dict and rv.dictiter is not None:
        return dict(rv.dictiter)
    # main method
    func = rv.func
    y = func(*rv.args)
    if rv.state is not None:
        state = rv.state
        if hasattr(y, "__setstate__"):
            y.__setstate__(state)
        else:
            if isinstance(state, tuple) and len(state) == 2:
                state, slotstate = state
            else:
                slotstate = None
            if state is not None:
                y.__dict__.update(state)
            if slotstate is not None:
                for key, value in slotstate.items():
                    setattr(y, key, value)
    if rv.listiter is not None:
        if hasattr(y, "extend"):
            y.extend(rv.listiter)
        else:
            for item in rv.listiter:
                y.append(item)
    if rv.dictiter is not None:
        items = rv.dictiter.items() if isinstance(rv.dictiter, dict) else rv.dictiter
        for key, value in items:
            y[key] = value
    return y


@dataclass
class Step:
    value: Any


@dataclass
class Stop:
    value: Any


VisitFn = Callable[[Any, Tuple[Any]], Union[Step, Stop]]

""" Closure of walk """


def traverse(
    x,
    *,
    pre: VisitFn = lambda x, path: Step(x),
    post: VisitFn = lambda x, path: Stop(x),
):
    def err(n, r):
        return TypeError(
            f"result of calling `{n}` should be a `Step` or `Stop` but got {type(r)}, use `traverse.stop` and `traverse.step`."
        )

    def rec(x, path=()):
        r = pre(x, path)
        if isinstance(r, Stop):
            return r.value
        elif isinstance(r, Step):
            x = r.value
        else:
            raise err("pre", r)
        x = walk(x, lambda y, k: rec(y, (*path, k)))
        r = post(x, path)
        if isinstance(r, Stop):
            return r.value
        elif isinstance(r, Step):
            return rec(x, path=path)
        else:
            raise err("post", r)

    return rec(x)


traverse.step = Step
traverse.stop = Stop

""" Similar to https://funcy.readthedocs.io/en/stable/colls.html#walk

Main difference is that the input is always `(value, key)` and the output is always `value`.
We also treat dataclasses as walkable.
"""


def walk(x, fn):
    if hasattr(x, "__walk__"):
        return x.__walk__(fn)
    rv = reduce(x)
    if rv is None:
        return x
    rv = rv.walk(fn)
    return reconstruct(rv)


"""
todos:

- [ ] is_atomic
- [ ] iter_children -- ''

 """
