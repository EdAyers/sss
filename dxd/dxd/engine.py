from abc import ABC
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Generator, Literal, Optional, Protocol, Type

from miniscutil.adapt import adapt, restore
from .expr import Expr, AbstractExpr


class Engine(ABC):
    """Database engine"""

    mode: Literal["sqlite", "postgresql"]

    @property
    def protocol(self):
        raise NotImplementedError()

    def execute_expr(self, expr: Expr) -> Any:
        t = expr.template
        if not t.rstrip().endswith(";"):
            t += ";"
        return self.execute(t, tuple(self.adapt(x) for x in expr.values))

    def execute(self, query: str, values: tuple[Any, ...] = ()) -> Any:
        raise NotImplementedError()

    def executemany(self, query: str, values: list[tuple[Any, ...]]):
        raise NotImplementedError()

    def transaction(self) -> Generator[None, None, None]:
        raise NotImplementedError()

    def get_storage_type(self, T: Type) -> str:
        raise NotImplementedError()

    def adapt(self, value):
        return adapt(value, self.protocol)

    def restore(self, T, value):
        return restore(T, value)

    def commit(self):
        raise NotImplementedError()

    # [todo] transaction context manager


engine_context: ContextVar[Engine] = ContextVar("engine")


@contextmanager
def engine(engine: Engine):
    t = engine_context.set(engine)
    try:
        yield engine
    finally:
        engine_context.reset(t)


@contextmanager
def transaction(engine: Optional[Engine] = None):
    engine = engine or engine_context.get()
    yield from engine.transaction()
