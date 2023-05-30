from asyncio import Future, StreamReader, StreamWriter, Task
import asyncio
from functools import singledispatch, partial
from dataclasses import MISSING, asdict, dataclass, field, is_dataclass
from enum import Enum
import logging
import sys
from typing import (
    Any,
    Awaitable,
    Dict,
    Generic,
    List,
    Optional,
    TypeVar,
    Union,
    Coroutine,
)
import inspect
import warnings

from miniscutil.ofdict import MyJsonEncoder, ofdict
import json
from .transport import Transport, TransportClosedError, TransportClosedOK

logger = logging.getLogger(__name__)


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

    ### Codes that I made up for UXU

    unauthorized = -32401
    """ The user is not authorized to make this request. """


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

    def __str__(self):
        if self.id is None:
            return f"notify {self.method}"
        else:
            return f"request {self.method}:{self.id}"


@dataclass
class ResponseError(Exception):
    code: ErrorCode
    message: str
    data: Optional[Any] = field(default=None)

    def __str__(self):
        return f"{self.code.name}: {self.message}"


def invalid_request(message: str) -> ResponseError:
    return ResponseError(ErrorCode.invalid_request, message)


def method_not_found(method_name: str) -> ResponseError:
    return ResponseError(
        ErrorCode.method_not_found,
        f"no method found for {method_name}",
        data=method_name,
    )


def invalid_params(message: str = "invalid params") -> ResponseError:
    return ResponseError(ErrorCode.invalid_params, message)


def internal_error(message: str) -> ResponseError:
    return ResponseError(ErrorCode.internal_error, message)


def server_not_initialized(message: str) -> ResponseError:
    return ResponseError(ErrorCode.server_not_initialized, message)


@dataclass
class Response:
    """JSON-RPC response.

    https://www.jsonrpc.org/specification#response_object
    """

    id: Optional[Union[str, int]] = field(default=None)
    result: Optional[Any] = field(default=None)
    error: Optional[ResponseError] = field(default=None)
    jsonrpc: str = field(default="2.0")

    def to_bytes(self):
        return encoder.encode(self).encode()


class Dispatcher:
    """Dispatcher for JSON-RPC requests."""

    def __init__(self, methods=None, extra_kwargs={}):
        self.methods = methods or {}
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
            if funcname in self.methods:
                warnings.warn(
                    f"method with name {funcname} already registered, overwriting"
                )
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


class InitializationMode(Enum):
    NoInit = 0
    """ No initialization required. """
    ExpectInit = 1
    """ We expect to receive an initialize request from the peer. """
    SendInit = 2
    """ We should send an initialize request to the peer. """


class RpcServerStatus(Enum):
    preinit = 0
    running = 1
    shutdown = 2


class ExitNotification(Exception):
    """Thrown when the server recieved an exit notifaction from its peer."""


def rpc_method(name: Optional[str] = None):
    """Decorate your method with this to say that you are implementing a JSON-RPC method."""

    def decorator(fn):
        setattr(fn, "rpc_method", name or fn.__name__)
        return fn

    return decorator


def rpc_request(name: Optional[str] = None):
    """Decorate your _stub_ method with this to have a client RPC."""

    def decorator(fn):
        assert asyncio.iscoroutinefunction(fn)
        fn_name = name or fn.__name__

        async def method(self, params):
            return await self.request(fn_name, params)

        return method

    return decorator


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
    [todo] instead of needing to explicitly register methods, make a decorator.
    """

    dispatcher: Dispatcher
    status: RpcServerStatus
    transport: Transport
    init_mode: InitializationMode
    name: str
    request_counter: int
    """ Unique id for each request I make to the peer. """
    my_requests: dict[RequestId, Future[Any]]
    """ Requests that I have made to my peer. """
    their_requests: dict[RequestId, Task]
    """ Requests that my peer has made to me. """
    notification_tasks: set[asyncio.Task]
    """ Tasks running from notifications that my peer has sent to me. """

    def __init__(
        self,
        transport: Transport,
        dispatcher=None,
        name=None,
        init_mode: InitializationMode = InitializationMode.NoInit,
    ):
        if not isinstance(transport, Transport):
            raise TypeError(
                f"transport must be an instance of {Transport.__module__}.Transport, not {type(transport)}"
            )
        global server_count
        server_count += 1
        if name is None:
            self.name = f"<{type(self).__name__} {server_count}>"
        else:
            assert isinstance(name, str)
            self.name = name
        self.init_mode = init_mode
        if init_mode == InitializationMode.NoInit:
            self.status = RpcServerStatus.running
        else:
            self.status = RpcServerStatus.preinit
        self.transport = transport
        self.dispatcher = dispatcher or Dispatcher()
        self.my_requests = {}
        self.their_requests = {}
        self.request_counter = 1000
        self.notification_tasks = set()

        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            rpc_method = getattr(method, "rpc_method", None)
            if rpc_method is not None:
                # [todo] assert that the signature is correct
                logger.debug(
                    f"registering RPC method '{rpc_method}' to {method.__qualname__}"
                )
                self.dispatcher.register(rpc_method)(method)

    def __str__(self):
        return self.name

    async def send(self, r: Union[Response, Request]):
        await self.transport.send(r.to_bytes())

    async def notify(self, method: str, params: Optional[Any]):
        if self.status != RpcServerStatus.running:
            raise RuntimeError(
                f"can't send notifications while server is in {self.status.name} state"
            )
        req = Request(method=method, params=params)
        await self.send(req)

    async def request(self, method: str, params: Optional[Any]) -> Any:
        if self.status != RpcServerStatus.running:
            if self.init_mode != InitializationMode.SendInit or method != "initialize":
                raise RuntimeError(
                    f"can't make new requests while server is in {self.status.name} state"
                )
        self.request_counter += 1
        id = self.request_counter
        req = Request(method=method, id=id, params=params)
        fut = asyncio.get_running_loop().create_future()
        # [todo] I think the pythonic way to do this is to have this dict be a weakref, and the
        # caller is responsible for holding the request object.
        # If the request future is disposed then we send a cancel request to client.
        if id in self.my_requests:
            raise RuntimeError(f"non-unique request id {id} found")
        self.my_requests[id] = fut
        await self.send(req)
        result = await fut
        return result

    async def _send_init(self, init_param):
        await self.request("initialize", init_param)
        self.status = RpcServerStatus.running
        await self.notify("initialized", None)

    async def serve_forever(self, init_param=None):
        """Runs forever. Serves your client.

        It will return when:
        - the transport closes gracefully
        - the exit notification is received.

        Raises:
            - TransportClosedError:the transport closes with an error
            - TransportError: some other error at the transport level occurred
        """
        # [todo] add a lock to prevent multiple server loops from running at the same time.

        if self.init_mode == InitializationMode.SendInit:
            if self.status != RpcServerStatus.preinit:
                raise RuntimeError(
                    f"can't start server while server is in {self.status.name} state"
                )
            if init_param is None:
                raise ValueError(
                    f"init_param must be provided in {self.init_mode.name} mode"
                )
            task = asyncio.create_task(self._send_init(init_param))
            self.notification_tasks.add(task)
            task.add_done_callback(self.notification_tasks.discard)
        try:
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
                except KeyboardInterrupt as e:
                    logger.exception(f"recieved kb interrupt, terminating")
                    return
        finally:
            logger.info(f"exiting serve_forever loop")
            (_, e, _) = sys.exc_info()  # sys.exception() is 3.11 only
            if e is None:
                e = ConnectionError(f"{self} shutdown")
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
                if self.status != RpcServerStatus.shutdown:
                    logger.warning("exit notification received before shutdown request")
                # https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#exit
                raise ExitNotification()
            if req.method == "shutdown":
                self._shutdown()
            task = asyncio.create_task(
                self._on_request(req),
                name=f"{self.name} handle {req}",
            )
            id = req.id
            if id is not None:
                if id in self.their_requests:
                    raise invalid_request(f"request id {id} is already in use")
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
            logger.exception(f"{self} {req} unhandled exception. data:\n{req.params}")
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
        if self.status == RpcServerStatus.preinit:
            INIT_METHOD = "initialize"
            if self.init_mode == InitializationMode.ExpectInit:
                if req.method == INIT_METHOD:
                    self.status = RpcServerStatus.running
                else:
                    raise server_not_initialized(
                        f"please request method {INIT_METHOD} before requesting anything else"
                    )
            elif self.init_mode == InitializationMode.SendInit:
                raise server_not_initialized(
                    f"please wait for me to send a {INIT_METHOD} request"
                )
            else:
                raise internal_error("invalid server state")
        if self.status == RpcServerStatus.shutdown:
            if req.method == "shutdown":
                if "shutdown" in self.dispatcher:
                    return await self.dispatcher.dispatch("shutdown", None)
                else:
                    return None
            raise invalid_request("server has shut down")

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
            logger.exception(message)
            raise invalid_params(message)
        result = await self.dispatcher.dispatch(req.method, params)
        return result
