import asyncio
from collections import deque
import logging
import queue
from threading import Thread
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Union,
)
import concurrent.futures
import os
import sys
import time

T = TypeVar("T")
U = TypeVar("U")


class StateStream(Generic[T]):
    value: T
    _events: set[asyncio.Event]

    def __init__(
        self,
        initial: T = None,
        should_update: Callable[[T, T], bool] = lambda x, y: True,
    ):
        self.value = initial
        self._events = set()
        self.should_update = should_update

    async def __aiter__(self):
        return self

    async def next(self):
        """Waits until the state's value changes. Always returns the most recent value."""
        return await self.__anext__()

    async def __anext__(self) -> T:
        e = asyncio.Event()
        self._events.add(e)
        await e.wait()
        self._events.discard(e)
        return self.value

    def set(self, value: T) -> None:
        if not self.should_update(self.value, value):
            return
        self.value = value
        for e in self._events:
            e.set()

    def update(self, modify: Callable[[T], T]) -> None:
        self.set(modify(self.value))


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

    def tolist(self):
        return list(self._items)

    async def wait_pop_many(self, limit=None):
        """Waits for at least one element to be in the queue and returns the whole queue.

        Args:
            limit(int | None): If an integer, will never return more than that many items.
        """
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

    async def wait_pop(self):
        xs = await self.wait_pop_many(limit=1)
        assert len(xs) == 1
        return xs[0]

    def pop_all(self):
        xs = self.tolist()
        self.clear()
        return xs

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.wait_pop()

    def clear(self):
        self._event.clear()
        self._items.clear()


class AsyncMap(AsyncIterable[T], Generic[T]):
    def __init__(self, map_fn: Callable[[U], T], aiter: AsyncIterator[U]):
        self.map_fn = map_fn
        self.aiter = aiter

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            x = await self.aiter.__anext__()
            return self.map_fn(x)


class AsyncFilter(AsyncIterable[T], Generic[T]):
    def __init__(self, predicate: Callable[[T], bool], aiter: AsyncIterator[T]):
        self.predicate = predicate
        self.aiter = aiter

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            x = await self.aiter.__anext__()
            if self.predicate(x):
                return x


# source: https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    try:
        logging.info("Daemon started.")
        loop.run_forever()
    finally:
        logging.info("Daemon shutting down.")
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logging.info("Daemon thread exiting.")


class Daemon:
    """Run asyncio operations like accessing sqlite and the internet in a separate thread.

    This is important because we never want HitSave to block the user's thread
    unless they are waiting on a result from the thread.
    We also don't want to use the user's potentially running asyncio event loop.
    """

    _thread: Thread
    _loop: asyncio.AbstractEventLoop

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._thread = Thread(
            target=start_background_loop, args=(self._loop,), daemon=True
        )
        self._thread.start()

    @classmethod
    def default(cls):
        return cls()

    def run_sync(
        self, coro: Coroutine[Any, Any, T], timeout: Optional[float] = None
    ) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def stop(self):
        self._loop.stop()

    def create_task(
        self, coro: Coroutine[Any, Any, T], name: Optional[str] = None
    ) -> asyncio.Task[T]:
        return self._loop.create_task(coro, name=name)


async def wrap_iterator(
    it: Union[Callable[[], Iterator[T]], Iterator[T]], executor=None
) -> AsyncIterator[T]:
    """Make a blocking iterator into an async iterator by running the iterator in a separate thread.

    We assume that the iterator is thread safe (motivating use case: iterating over a sqlite cursor).
    If argument is a callable, then we run the callable in a separate thread too.
    We also assume that a thread pool executor is set.
    """
    loop = asyncio.get_running_loop()
    if callable(it):
        it = await loop.run_in_executor(executor, it)
    while True:
        result: Any = await loop.run_in_executor(executor, next, it, StopIteration)
        if result is StopIteration:
            return
        yield result
