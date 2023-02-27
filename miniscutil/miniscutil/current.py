from contextvars import ContextVar
import logging
from typing import List, TypeVar, Type

T = TypeVar("T", bound="Current")


class Current:
    """A mixin for classes where you want there to be a 'current' instance.
    You can get the current instance by calling ``cls.current``
    """

    CURRENT: ContextVar
    _tokens: List

    @classmethod
    def default(cls):
        """Override this to create a default value for current."""
        # [todo] whatever abc magic is needed here for static analysis.
        raise NotImplementedError(f"{cls.__qualname__}.default() is not implemented.")

    def __init_subclass__(cls):
        cls.CURRENT = ContextVar(cls.__qualname__ + ".CURRENT")
        # ref: https://docs.python.org/3/reference/datamodel.html#object.__init_subclass__

    def __enter__(self):
        if not hasattr(self, "_tokens"):
            self._tokens = []
        self._tokens.append(self.__class__.CURRENT.set(self))
        return self

    def __exit__(self, ex_type, ex_value, ex_trace):
        assert hasattr(self, "_tokens")
        assert len(self._tokens) > 0
        t = self._tokens.pop()
        self.__class__.CURRENT.reset(t)

    @classmethod
    def current(cls: Type[T]) -> T:
        """The current value of the singleton class."""
        c = cls.CURRENT.get(None)
        if c is None:
            try:
                c = cls.default()
            except NotImplementedError:
                c = cls()
            assert isinstance(c, cls)
            cls.CURRENT.set(c)
        return c

    @classmethod
    def set_current(cls, t, exist_ok=False):
        if (cls.CURRENT.get(None) is not None) and not exist_ok:
            logging.warning(
                f"{cls.__qualname__} already has a current value. Overwriting."
            )
        return cls.CURRENT.set(t)
