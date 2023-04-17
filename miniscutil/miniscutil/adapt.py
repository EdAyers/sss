from collections import defaultdict
from contextvars import ContextVar
import datetime
from enum import Enum
from functools import singledispatch
from typing import Any, Callable, NewType, Type, get_origin
import uuid

from .dispatch import Dispatcher, classdispatch
from .type_util import as_newtype, as_optional

""" Implementation of PEP-246 https://peps.python.org/pep-0246/#specification

I'm not sure it quite captures the spirit of the PEP because adapt doesn't necessarily _wrap_ the input.
"""


class AdaptationError(TypeError):
    pass


class LiskovViolation(AdaptationError):
    pass


adapters: dict[Type, Dispatcher[Callable[[Any], Any]]] = defaultdict(Dispatcher)


def register_adapter(T: Type, protocol: Type):
    def core(f: Callable[[Any], Any]):
        dispatcher = adapters[protocol]
        dispatcher.register(T, f)
        return f

    return core


def adapt(obj, protocol):
    """Turn it into a compatible type."""
    t = type(obj)
    if t is protocol:
        return obj
    if protocol is None:
        raise ValueError("No protocol specified")
    try:
        conform = getattr(obj, "__conform__", None)
        if conform is not None:
            r = conform(protocol)
            if r is not NotImplemented:
                return r
        adapt = getattr(protocol, "__adapt__", None)
        if adapt is not None:
            r = adapt(obj)
            if r is not NotImplemented:
                return r
    except LiskovViolation:
        pass
    else:
        if isinstance(obj, protocol):
            return obj
    f = adapters[protocol].dispatch(t)
    if f is not None:
        r = f(obj)
        if r is not NotImplemented:
            return r
    raise AdaptationError(f"No adapter found for {t} and {protocol}")


@classdispatch
def restore(X, x):
    """Inverse of adapt.

    Args:
        X(type): The type to restore x to.
        x(object): The object to restore.
    """
    # [todo] abstract this out into an adapter pattern. adapt(x, protocol = X)

    Y = as_optional(X)
    if Y is not None:
        if x is None:
            return x
        else:
            return restore(Y, x)

    S = as_newtype(X)
    if S is not None:
        r = restore(S, x)
        return X(r)

    if get_origin(X) is None and isinstance(x, X):
        return x
    adapt = getattr(X, "__adapt__", None)
    if adapt is not None:
        r = adapt(x)
        if r is not NotImplemented:
            return r

    if issubclass(X, Enum):
        return X(x)
    if X is bool:
        return bool(x)
    raise NotImplementedError(f"Unsupported target type {X}")


# [todo] use pydantic
@restore.register(datetime.datetime)
def _restore_datetime(T, d):
    if isinstance(d, datetime.datetime):
        return d
    elif isinstance(d, str):
        return datetime.datetime.fromisoformat(d)
    else:
        raise TypeError(f"Unsupported datetime type {type(d)}")


@restore.register(uuid.UUID)
def _restore_uuid(T, b):
    if isinstance(b, uuid.UUID):
        return b
    elif isinstance(b, bytes):
        return uuid.UUID(bytes=b)
    else:
        raise TypeError(f"Unsupported uuid type {type(b)}")
