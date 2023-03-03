from asyncio import Future, StreamReader, StreamWriter, Task
import asyncio
from functools import singledispatch, partial
from dataclasses import MISSING, asdict, dataclass, field, is_dataclass
from enum import Enum
import logging
import sys
from typing import Any, Awaitable, Dict, List, Optional, Union, Coroutine
import inspect

from miniscutil.ofdict import MyJsonEncoder, ofdict
import json
from miniscutil.rpc.transport import Transport, TransportClosedError, TransportClosedOK

from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

logger = logging.getLogger("jsonrpc")


class ErrorCode(Enum):
    ### JSON-RPC codes

    parse_error = -32700
    """ It doesn't parse as JSON """
    invalid_request = -32600
    """ The JSON sent is not a valid Request object. """
    method_not_found = -32601
    """ The method does not exist / is not available. """
    invalid_params = -32602
    """ Your parameters are not valid. (eg fields missing, bad types etc) """
    internal_error = -32603
    """ The internal JSON-RPC server code messed up. """

    ### Codes specific to LSP

    server_error = -32000
    """ There was a problem in the method handler. """

    server_not_initialized = -32002
    """ The server has not been initialized. """

    request_failed = -32803
    """A request failed but it was syntactically correct, e.g the
	 * method name was known and the parameters were valid. The error
	 * message should contain human readable information about why
	 * the request failed.
	 *"""

    server_cancelled = -32802
    """The server cancelled the request. This error code should
	 * only be used for requests that explicitly support being
	 * server cancellable."""

    content_modified = -32801
    """ Content got modified outside of normal conditions. """
    request_cancelled = -32800
    """ The client cancelled a request and the server has detected the cancel. """


encoder = MyJsonEncoder()


@dataclass
class Request:
    method: str
    id: Optional[Union[str, int]] = field(default=None)
    params: Optional[Any] = field(default=None)

    @property
    def is_notification(self):
        return self.id is None

    def to_bytes(self):
        return encoder.encode(self).encode()


@dataclass
class ResponseError(Exception):
    code: ErrorCode
    message: str
    data: Optional[Any] = field(default=None)

    def __str__(self):
        return f"{self.code.name}: {self.message}"


def invalid_request(message: str) -> ResponseError:
    return ResponseError(ErrorCode.invalid_request, message)


def method_not_found(message: str) -> ResponseError:
    return ResponseError(ErrorCode.method_not_found, message)


def invalid_params(message: str) -> ResponseError:
    return ResponseError(ErrorCode.invalid_params, message)


@dataclass
class Response:
    """JSON-RPC response.

    https://www.jsonrpc.org/specification#response_object
    """

    id: Any = field(default=None)
    result: Optional[Any] = field(default=None)
    error: Optional[ResponseError] = field(default=None)
    jsonrpc: str = field(default="2.0")

    def to_bytes(self):
        return encoder.encode(self).encode()


class Dispatcher:
    """Dispatcher for JSON-RPC requests."""

    def __init__(self, methods={}, extra_kwargs={}):
        self.methods = methods
        self.extra_kwargs = extra_kwargs

    def __contains__(self, method):
        return method in self.methods

    def __getitem__(self, method):
        return partial(self.methods[method], **self.extra_kwargs)

    def param_type(self, method):
        fn = self.methods[method]
        sig = inspect.signature(fn)
        if len(sig.parameters) == 0:
            T = Any
        else:
            P = next(iter(sig.parameters.values()))
            T = P.annotation
            if T is inspect.Parameter.empty:
                T = Any
        return T

    def return_type(self, method):
        fn = self.methods[method]
        sig = inspect.signature(fn)
        a = sig.return_annotation
        if a is inspect.Signature.empty:
            return Any
        else:
            return a

    def register(self, name=None):
        def core(fn):
            funcname = name or fn.__name__
            self.methods[funcname] = fn
            return fn

        return core

    def with_kwargs(self, **kwargs):
        return Dispatcher(self.methods, {**self.extra_kwargs, **kwargs})

    async def dispatch(self, method: str, params: Any):
        fn = self[method]
        result = fn(params)
        if asyncio.iscoroutine(result):
            result = await result
        return result


server_count = 0

RequestId = Union[str, int]


class RpcServerStatus(Enum):
    running = 1
    shutdown = 2


class ExitNotification(Exception):
    """Thrown when the server recieved an exit notifaction from its peer."""


class RpcServer:
    """Implementation of a JSON-RPC server.

    Following the conventions of LSP for extra functionality.

    Builtin methods:
    - "$/cancelRequest" notification will cancel the given request id.
      https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#cancelRequest
    - "shutdown" will put the server into a shutdown state.
      The 'shutdown' handler on the dispatcher will be called.
      All in-flight requests will be cancelled.
      No further requests or notifications will be sent.
      https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#shutdown
    - "exit" will cause the server to exit immediately (without replying to requests)

    [todo] special case for initialized; don't let other methods be called until initialized method is called.
    [todo] implement progress support
    [todo] rename to RpcConnection, then RpcServer and RpcClient handle the different conventions for
    lifecycle.
    [todo] add warnings if requests go unanswered for too long.
    """

    dispatcher: Dispatcher
    status: RpcServerStatus
    transport: Transport
    request_counter: int
    """ Unique id for each request I make to the peer. """
    my_requests: Dict[RequestId, Future[Any]]
    """ Requests that I have made to my peer. """
    their_requests: Dict[RequestId, Task]
    """ Requests that my peer has made to me. """
    notification_tasks: set[asyncio.Task]
    """ Tasks running from notifications that my peer has sent to me. """

    def __init__(
        self,
        transport: Transport,
        dispatcher=Dispatcher(),
        name=None,
        status=RpcServerStatus.running,
    ):
        global server_count
        server_count += 1
        if name is None:
            self.name = f"<{type(self).__name__} {server_count}>"
        else:
            self.name = name
        self.status = status
        self.transport = transport
        self.dispatcher = dispatcher
        self.my_requests = {}
        self.their_requests = {}
        self.request_counter = 1000
        self.notification_tasks = set()

    def __str__(self):
        return self.name

    async def send(self, r: Union[Response, Request]):
        await self.transport.send(r.to_bytes())

    async def notify(self, method: str, params: Optional[Any]):
        req = Request(method=method, params=params)
        await self.send(req)

    async def request(self, method: str, params: Optional[Any]) -> Any:
        self.request_counter += 1
        id = self.request_counter
        req = Request(method=method, id=id, params=params)
        fut = asyncio.get_running_loop().create_future()
        self.my_requests[id] = fut
        await self.send(req)
        result = await fut
        return result

    async def serve_forever(self):
        """Runs forever. Serves your client.

        It will return when:
        - the transport closes gracefully
        - the exit notification is received.

        [todo] add param for initialise message to send _or_ flag indicating an initialisation message is expected first.

        Raises:
            - TransportClosedError:the transport closes with an error
            - TransportError: some other error at the transport level occurred
        """
        while True:
            try:
                data = await self.transport.recv()
                messages = json.loads(data)
                # res can be a batch
                if isinstance(messages, dict):
                    messages = [messages]
                elif not isinstance(messages, list):
                    raise TypeError(f"expected list or dict, got {type(messages)}")
                for message in messages:
                    self._handle_message(message)
            except TransportClosedOK as e:
                logger.info(f"{self.name} transport closed gracefully: {e}")
                return
            except (json.JSONDecodeError, TypeError) as e:
                logger.exception("invalid json")
                response = Response(
                    error=ResponseError(message=str(e), code=ErrorCode.parse_error)
                )
                await self.send(response)
                continue
            except ExitNotification as e:
                logger.info(f"{self.name} received exit notification, terminating")
                return
            except TransportClosedError as e:
                raise e
            except Exception as e:
                raise e
            finally:
                e = sys.exception() or ConnectionError(f"{self} shutdown")
                for fut in self.my_requests.values():
                    fut.set_exception(e)
                self._shutdown()

    def _shutdown(self):
        for t in self.their_requests.values():
            t.cancel("shutdown")
        for t in self.notification_tasks:
            t.cancel("shutdown")
        self.status = RpcServerStatus.shutdown

    def _handle_message(self, message: Any):
        if "result" in message or "error" in message:
            # this is a Response
            res = ofdict(Response, message)
            if res.id not in self.my_requests:
                logger.error(f"received response for unknown request: {res}")
                return
            fut = self.my_requests.pop(res.id)
            if res.error is not None:
                fut.set_exception(res.error)
            else:
                fut.set_result(res.result)
        else:
            # this is a request.
            req = ofdict(Request, message)
            if req.method == "exit":
                if not self.status == RpcServerStatus.shutdown:
                    logger.warning("exit notification received before shutdown request")
                # https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#exit
                raise ExitNotification()
            if req.method == "shutdown":
                self._shutdown()
            task = asyncio.create_task(
                self._on_request(req),
                name=f"{self.name} handle {req.method} {req.id or ''} ",
            )
            id = req.id
            if id is not None:
                self.their_requests[id] = task
                task.add_done_callback(lambda _: self.their_requests.pop(id))
            else:
                self.notification_tasks.add(task)
                task.add_done_callback(self.notification_tasks.discard)

    async def _on_request(self, req: Request) -> None:
        try:
            result = await self._on_request_core(req)
        except asyncio.CancelledError as e:
            if not req.is_notification:
                await self.send(
                    Response(
                        id=req.id,
                        error=ResponseError(
                            code=ErrorCode.request_cancelled, message=str(e)
                        ),
                    )
                )
        except ResponseError as e:
            await self.send(Response(id=req.id, error=e))
        except Exception as e:
            logger.exception(f"{self} {req.id} unhandled exception")
            await self.send(
                Response(
                    id=req.id,
                    error=ResponseError(code=ErrorCode.server_error, message=str(e)),
                )
            )
        else:
            if not req.is_notification:
                await self.send(Response(id=req.id, result=result))
            else:
                assert result is None, "notification handlers must return None"

    async def _on_request_core(self, req: Request):
        if self.status == RpcServerStatus.shutdown:
            if req.method == "shutdown":
                if "shutdown" in self.dispatcher:
                    return await self.dispatcher.dispatch("shutdown", None)
                else:
                    return None
            raise invalid_request("server has shut down")

        logger.debug(f"{self.name} ← {req.id} {req.method}")

        if req.method == "$/cancelRequest":
            if not req.is_notification:
                raise invalid_request("cancel request must be a notification")
            if not isinstance(req.params, dict) or not "id" in req.params:
                raise invalid_params('params must be a dict with "id" key')
            t = self.their_requests.get(req.params["id"], None)
            if t is not None:
                t.cancel("requested by peer")
            return None

        if req.method not in self.dispatcher:
            raise method_not_found(req.method)

        T = self.dispatcher.param_type(req.method)
        try:
            params = ofdict(T, req.params)
        except TypeError as e:
            message = (
                f"{req.method} {type(e).__name__} failed to decode params to {T}: {e}"
            )
            raise invalid_params(message)
        result = await self.dispatcher.dispatch(req.method, params)
        return result