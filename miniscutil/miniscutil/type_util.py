import inspect
from typing import (
    Any,
    NewType,
    get_origin,
    get_args,
    Type,
    Optional,
    Union,
)


def is_optional(T: Type) -> bool:
    """Returns true if ``T == Union[NoneType, _] == Optional[_]``."""
    return as_optional(T) is not None


def as_optional(T: Type) -> Optional[Type]:
    """If we have ``T == Optional[X]``, returns ``X``, otherwise returns ``None``.

    Note that because ``Optional[X] == Union[X, type(None)]``, so
    we have ``as_optional(Optional[Optional[X]]) ↝ X``
    ref: https://stackoverflow.com/questions/56832881/check-if-a-field-is-typing-optional
    """
    if get_origin(T) is Union:
        args = get_args(T)
        if type(None) in args:
            ts = tuple(a for a in args if a is not type(None))
            if len(ts) == 0:
                return None
            if len(ts) == 1:
                return ts[0]
            else:
                return Union[ts]  # type: ignore
    return None


def as_list(T: Type) -> Optional[Type]:
    """If `T = List[X]`, return `X`, otherwise return None."""
    if T == list:
        return Any
    o = get_origin(T)
    if o is None:
        return None
    if issubclass(o, list):
        return get_args(T)[0]
    return None


def as_newtype(T: Type) -> Optional[Type]:
    return getattr(T, "__supertype__", None)


def as_set(T: Type) -> Optional[Type]:
    if T == set:
        return Any
    o = get_origin(T)
    if o is None:
        return None
    if issubclass(o, set):
        return get_args(T)[0]
    return None


def is_subtype(T1: Type, T2: Type) -> bool:
    """Polyfill for issubclass.

    Pre 3.10 doesn't have good support for subclassing unions.
    """
    try:
        return issubclass(T1, T2)
    except TypeError:
        if get_origin(T2) is Union:
            return any(is_subtype(T1, a) for a in get_args(T2))
        S = as_newtype(T2)
        if S is not None:
            return is_subtype(T1, S)
        raise
