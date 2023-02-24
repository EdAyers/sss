from typing import (
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
    o = get_origin(T)
    if o is None:
        return None
    if issubclass(o, list):
        return get_args(T)[0]
    return None
