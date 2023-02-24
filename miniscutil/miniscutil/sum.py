import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Generic, Iterable, TypeVar


A = TypeVar("A")
B = TypeVar("B")
R = TypeVar("R")
S = TypeVar("S")
T = TypeVar("T")


@dataclass
class Sum(Generic[A, B]):
    """Inductive sum type."""

    is_left: bool
    value: Any

    @classmethod
    def inl(cls, value: A):
        return cls(True, value)

    @classmethod
    def inr(cls, value: B):
        return cls(False, value)

    def match(self, left: Callable[[A], R], right: Callable[[B], R]) -> R:
        return left(self.value) if self.is_left else right(self.value)

    def map(self, left: Callable[[A], R], right: Callable[[B], S]) -> "Sum[R, S]":
        return Sum.inl(left(self.value)) if self.is_left else Sum.inr(right(self.value))  # type: ignore

    def mapl(self, f: Callable[[A], R]) -> "Sum[R,B]":
        return Sum.inl(f(self.value)) if self.is_left else Sum.inr(self.value)  # type: ignore

    def mapr(self, f: Callable[[B], R]) -> "Sum[A,R]":
        return Sum.inl(self.value) if self.is_left else Sum.inr(f(self.value))  # type: ignore

    def __hash__(self) -> int:
        return hash((self.is_left, self.value))

    def __todict__(self):
        return [0 if self.is_left else 1, self.value]

    @classmethod
    def __ofdict__(cls, d):
        assert isinstance(d, list) and len(d) == 2
        return cls(bool(d[0]), d[1])


class MessageQueue(Generic[T]):
    """Simple message queue where you can push messages to the queue.

    Messages are popped exactly once (ie this is not fanout or pub/sub).
    Not thread safe.
    """

    _items: deque[T]
    _event: asyncio.Event

    def __init__(self):
        self._items = deque()
        self._event = asyncio.Event()

    def push(self, *items: T) -> None:
        self.pushes(items)

    def pushes(self, items: Iterable[T]):
        self._items.extend(items)
        if len(self) > 0:
            self._event.set()

    def __len__(self):
        return len(self._items)

    async def pop_many(self, limit=None):
        if len(self._items) == 0:
            await self._event.wait()
        assert len(self._items) > 0
        if (limit is None) or (0 < len(self._items) <= limit):
            result = list(self._items)
            self._items.clear()
        else:
            assert limit > 0
            result = []
            while len(result) < limit:
                result.append(self._items.popleft())
        if len(self._items) == 0:
            self._event.clear()
        return result

    async def pop(self):
        xs = await self.pop_many(limit=1)
        assert len(xs) == 1
        return xs[0]

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.pop()

    def clear(self):
        self._event.clear()
        self._items.clear()
