from abc import get_cache_token
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Type,
    TypeVar,
    Union,
    get_origin,
    NewType,
)
from functools import singledispatch, update_wrapper
from functools import _find_impl  # type: ignore
from weakref import WeakKeyDictionary

F = TypeVar("F")


class Dispatcher(Generic[F]):
    """Reimplementation of the dispatching logic for functools.singledispatch."""

    registry: Dict[Type, F]
    cache: WeakKeyDictionary
    cache_token: Optional[object]

    def __init__(self):
        self.registry = {}
        self.cache = WeakKeyDictionary()
        self.cache_token = None

    def register(self, cls: Type, f: Optional[F] = None):
        # [todo] method override
        if f is not None:
            self.registry[cls] = f
        else:

            def x(f):
                self.registry[cls] = f
                return f

            return x

    def update(self, cls: Type, modifier: Callable[[Optional[F]], F]) -> None:
        r = self.get(cls)
        r = modifier(r)
        self.register(cls, r)
        self.cache.clear()

    def __contains__(self, cls):
        return self.get(cls) is not None

    def __getitem__(self, cls):
        return self.get(cls)

    def dispatch(self, cls) -> Optional[F]:
        return self.get(cls)

    def get(self, cls) -> Union[F, None]:
        """generic_func.dispatch(cls) -> <function implementation>

        Runs the dispatch algorithm to return the best available implementation
        for the given *cls* registered on *generic_func*.

        """
        # [todo] also dispatch on generic aliases.
        if self.cache_token is not None:
            current_token = get_cache_token()
            if self.cache_token != current_token:
                self.cache.clear()
                self.cache_token = current_token
        try:
            impl = self.cache[cls]
        except KeyError:
            try:
                impl = self.registry[cls]
            except KeyError:
                try:
                    impl = _find_impl(cls, self.registry)
                except KeyError:
                    impl = self.registry[Any]
            self.cache[cls] = impl
        return impl


def classdispatch(func):
    """Dynamic dispatch on a class.

    Similar to ``functools.singledispatch``, except treats the first argument as a class to be dispatched on.
    """
    # [todo] switch to using dispatcher
    # [todo] add support for register(Optional) and register(Union) and register(list[int]).
    funcname = getattr(func, "__name__", "class dispatch function")
    sdfunc = singledispatch(func)

    def dispatch(cls):
        g = sdfunc.registry.get(cls)
        if g is not None:
            return g
        orig = get_origin(cls)
        if orig is not None:
            g = sdfunc.registry.get(orig)
            if g is not None:
                return g
            cls = orig
        try:
            return sdfunc.dispatch(cls)
        except Exception:
            return sdfunc.dispatch(object)

    def wrapper(*args, **kwargs):
        if not args:
            raise TypeError(f"{funcname} requires at leat one positional argument.")
        cls = args[0]
        return dispatch(cls)(*args, **kwargs)

    for n in ["register", "registry"]:
        setattr(wrapper, n, getattr(sdfunc, n))
    setattr(wrapper, "dispatch", dispatch)
    update_wrapper(wrapper, func)
    return wrapper
