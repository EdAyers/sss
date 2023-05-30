import asyncio
from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Generic,
    Optional,
    Union,
)
from uuid import uuid4

try:
    from typing import TypeAlias, TypeVar
except:
    from typing_extensions import TypeAlias, TypeVar
from .transport import Transport
from .jsonrpc import InitializationMode, RpcServer, rpc_method

ProgressToken: TypeAlias = Union[str, int]

logger = logging.getLogger(__name__)


class WorkDoneProgressParams:
    workDoneToken: Optional[ProgressToken]


P = TypeVar("P", bound="WorkDoneProgressParams")

Q = TypeVar("Q")
R = TypeVar("R")


@dataclass
class ProgressNotification(Generic[Q]):
    token: ProgressToken
    value: Q


class RequestFutureWithProgress(Generic[Q, R]):
    """A future for an RPC request that also has a 'progress()' field."""

    token: ProgressToken
    _q: asyncio.Queue[Union[Q, StopAsyncIteration]]
    _r: asyncio.Future[R]
    _callbacks: set[Callable[[Q], None]]

    def __init__(self, token: ProgressToken, result_fut: asyncio.Future[R]):
        # [todo] use a deque and Event instead so that it can be iterated multiple times.
        self.token = token
        self._q = asyncio.Queue()
        self._r = result_fut
        self._r.add_done_callback(
            lambda _: self._q.put_nowait(StopAsyncIteration("request done"))
        )

    def _put(self, progress_item: Q):
        if self._r.done():
            logger.warning(
                f"tried to put a progress item after request was done, this item will be ignored"
            )
            return
        for cb in self._callbacks:
            cb(progress_item)
        self._q.put_nowait(progress_item)

    def add_progress_callback(self, callback: Callable[[Q], None]):
        self._callbacks.add(callback)

    def remove_progress_callback(self, callback: Callable[[Q], None]):
        self._callbacks.discard(callback)

    def __await__(self):
        """Waits for the task to be done"""
        return self._r.__await__()

    async def progress(self) -> AsyncIterator[Q]:
        """An async iterator that reports all progress towards the goal and then ends when the goal is reached.

        Usage:
        ```
        async for item in ticket.progress():
            ...
        ```
        When the task is done, all of the progress will still be yielded before the loop exits.

        """
        while True:
            progress = await self._q.get()
            if isinstance(progress, StopAsyncIteration):
                return
            else:
                yield progress


class ExtraRpc(RpcServer):
    """There are a number of features that the LSP provides that are useful
    outside of the LSP protocol but which are not part of JSON-RPC.

    - Lifecycle events: startup, shutdown, initialization
    - Progress reporting
    - Request cancelling
    - Registering capabilities
    - Method discovery

    [todo] currently lifecycle and cancelling are implemented in
    RpcServer, need to move them here.

    """

    _my_progress: defaultdict[ProgressToken, set[RequestFutureWithProgress]]

    def __init__(
        self,
        transport: Transport,
        init_mode: InitializationMode = InitializationMode.NoInit,
    ):
        super().__init__(transport, init_mode=init_mode)
        self._my_progress = defaultdict(set)

    def request_with_progress(self, method: str, params: WorkDoneProgressParams):
        """Same as `request` but params include a 'progress token' that lets
        the method report status updates back.."""
        # [todo] the same request can have multiple progress tokens set.
        token = params.workDoneToken
        if token is not None:
            token = uuid4().hex
            params.workDoneToken = token
        assert token is not None
        if token in self._my_progress:
            logger.debug(f"multiple requests with same progress token: {token}")
            # note that this is ok, sometimes lots of requests get the same progress updates.
            # eg lots of requests might all depend on the same upstream task, and the progress bar is the same for all of them.
        fut = asyncio.create_task(self.request(method, params))
        rfwp = RequestFutureWithProgress(token, fut)
        # [todo] probably should be weakref with cancels
        self._my_progress[token].add(rfwp)
        fut.add_done_callback(lambda _: self._my_progress[token].discard(rfwp))
        return rfwp

    @rpc_method("$/progress")
    def handle_progress_notification(self, params: ProgressNotification):
        token = params.token
        for rfwp in self._my_progress[token]:
            rfwp._put(params.value)

    async def send_progress_notification(self, token: ProgressToken, value: Any):
        await self.notify("$/progress", ProgressNotification(token, value))

    # [todo] support throttling progress reporting
    # [todo] common progress use cases like partial results and progress bars.
    # [todo] make sure cancellations are properly handled with progress reporting.
    # [todo] move $/cancelRequest handler here
