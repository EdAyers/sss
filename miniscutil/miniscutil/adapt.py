from collections import defaultdict
from contextvars import ContextVar
import datetime
from enum import Enum
from functools import singledispatch
import inspect
from typing import Any, Callable, NewType, Type, TypeVar, Union, get_origin
import uuid

try:
    from typing import TypeAlias, TypeVar
except:
    from typing_extensions import TypeAlias, TypeVar
from .dispatch import Dispatcher, classdispatch
from .type_util import as_newtype, as_optional

""" Implementation of PEP-246 https://peps.python.org/pep-0246/#specification

I'm not sure it quite captures the spirit of the PEP because adapt doesn't necessarily _wrap_ the input.

To summarise, we introduce a new function `adapt(x, protocol)` which attempts to return a casted or adapted version of x that conforms to the protocol.
The protocol object could be a type, or it could be a `typing.Protocol` object, or it could just be a sentinel object representing a protocol.
For example, there is no expressible type for the valid input to json.dumps, but we can define a protocol object that represents the valid input to json.dumps.

When we call `adapt(x, protocol)`, the following things happen:

1. If x is already an instance of protocol (ie typechecks to protocol), return x.
2. If x has a __conform__ method, call it with protocol as an argument. If it returns NotImplemented, continue.
3. If protocol has an __adapt__ method, call it with x as an argument. If it returns NotImplemented, continue.
4. Lookup (type(x), protocol) in the adapter table, and call the function if it exists. If it returns NotImplemented, continue.
5. raise AdaptationError
"""

T = TypeVar("T")
Proto = TypeVar("Proto")


class AdaptationError(TypeError):
    pass


class LiskovViolation(AdaptationError):
    pass


adapters: dict[Type, Dispatcher[Callable[[Any, Any], Any]]] = defaultdict(Dispatcher)

AdapterFn: TypeAlias = Union[Callable[[Any, Any], Any], Callable[[Any], Any]]


def register_adapter(T: Type, protocol: Any):
    def core(f: AdapterFn):
        dispatcher = adapters[protocol]
        params = inspect.signature(f).parameters
        if len(params) == 1:
            a = lambda x, p: f(x)  # type: ignore
        elif len(params) != 2:
            raise TypeError("Adapter function must take 1 or 2 arguments")
        else:
            a = f
        dispatcher.register(T, a)  # type: ignore
        return f

    return core


def adapt(obj: Any, protocol: Type[Proto]) -> Proto:
    """Turn the given obj into something that conforms to the given protocol.

    There are 3 ways to register a handler for the adapter pattern:
    - implement a __conform__ method on the object
    - implement a __adapt__ method on the protocol
    - register a function with register_adapter

    Also note that adapt will automatically handle the case where protocol is Optional, NewType
    """
    t = type(obj)
    if t is protocol:
        return obj
    if protocol is None:
        raise ValueError("No protocol specified")

    try:
        conform_fn = getattr(obj, "__conform__", None)
        if conform_fn is not None:
            r = conform_fn(protocol)
            if r is not NotImplemented:
                return r
        adapt_fn = getattr(protocol, "__adapt__", None)
        if adapt_fn is not None:
            r = adapt_fn(obj)
            if r is not NotImplemented:
                return r
    except LiskovViolation:
        pass
    else:
        try:
            if isinstance(obj, protocol):
                return obj
        except TypeError:
            pass

    f = adapters[protocol].dispatch(t)
    if f is not None:
        r = f(obj, protocol)
        if r is not NotImplemented:
            return r
    # Optional case
    X = as_optional(protocol)
    if X is not None:
        if obj is None:
            return None  # type: ignore
        else:
            return adapt(obj, X)
    # unwrap newtypes
    S = as_newtype(protocol)
    if S is not None:
        r = adapt(obj, S)
        return S(r)
    # enums
    # [todo] enums should be handled by register_adapter (but atm there is no subclassing dispatcher on protocol variable.)
    if issubclass(protocol, Enum):
        if isinstance(obj, protocol):
            return obj
        if isinstance(obj, (int, str)):
            return protocol(obj)
    # [todo] unions
    raise AdaptationError(f"No adapter found for {t} and {protocol}")


def restore(X: Type[T], x: Any) -> T:
    """Inverse of adapt. It converts the object X to be a member of type x

    [todo] DEPRECATED.

    This calls adapt(x, X), but we add some extra steps because X is a type, and not a protocol.
    It automatically handles the case where X is Optional, NewType, or an Enum.


    Args:
        X(type): The type to restore x to.
        x(object): The object to restore.
    """
    return adapt(x, X)


@register_adapter(str, datetime.datetime)
def _adapt_str_to_datetime(x):
    return datetime.datetime.fromisoformat(x)


@register_adapter(str, uuid.UUID)
def _adapt_str_to_uuid(x):
    assert isinstance(x, str)
    return uuid.UUID(hex=x)


@register_adapter(bytes, uuid.UUID)
def _adapt_bytes_to_uuid(x):
    return uuid.UUID(bytes=x)
