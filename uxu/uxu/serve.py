import asyncio
from datetime import datetime, timedelta
from secrets import token_urlsafe
from typing import Any, Callable, Optional
from uuid import UUID
from fastapi import Request, WebSocket, APIRouter
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, BaseSettings, Field, SecretStr
from uxu.manager import EventArgs, Manager
from uxu.rpc import WebsocketTransport, RpcServer, InitializationMode, Transport
from uxu.__about__ import __version__
from uxu.rpc.jsonrpc import invalid_params, rpc_method
from uxu.html import h
import starlette.routing
from starlette.responses import HTMLResponse
import dominate
import dominate.tags as t
from jose import ExpiredSignatureError, JWTError, jwt
import logging

# https://fastapi.tiangolo.com/advanced/websockets/?h=websocket

logger = logging.getLogger("uxu")


def uxu_root(request: Request, component, props):
    cfg = Settings()  # type: ignore
    expires_delta = cfg.jwt_expires
    expire = datetime.utcnow() + expires_delta
    # [todo] get subject as id of authenticated user.
    claims = TicketClaims(
        exp=expire,
        iat=datetime.utcnow(),
        jti=token_urlsafe(16),
        aud=request.url_for("uxu_socket"),
        iss=str(request.url.path),
        # [todo] how to identify the client? needs to defend csrf and xss
        # one option is simply that they have to log in first, then pass user_id
        sub=None,
    )
    ticket = jwt.encode(
        claims=claims.dict(),
        key=cfg.jwt_secret.get_secret_value(),
        algorithm=cfg.jwt_algorithm,
    )
    document: Any = dominate.document(title=f"Uxu {__version__}")
    with document.head:
        t.link(
            rel="stylesheet",
            href="https://unpkg.com/tachyons@4.12.0/css/tachyons.min.css",
        )
    with document.body:
        t.main("welcome to uxu...", id="uxu_root")
        t.script(
            f"UXU_TICKET = '{ticket}'; UXU_URL = '{request.url_for('uxu_socket')}';",
            type="text/javascript",
        )
        t.script(
            type="text/javascript", src=request.url_for("static", filename="uxu.js")
        )

    content = document.render()
    return HTMLResponse(content=content)


class UxuRouter:
    def __init__(self):
        self.routes = []

    def add_route(self, path: str, component: Callable):
        def endpoint(request):
            path_params = request.path_params
            # [todo] use fastapi signature introspection stuff.
            spec = h(component, **path_params)
            with Manager(spec) as mgr:
                rs = mgr.render()
            doc = dominate.document(title=f"Uxu {__version__}")
            for r in rs:
                doc.add(r.static())
            content = doc.render()
            response = HTMLResponse(content=content)
            # [todo] here, start a background task which is a websocket listener
            # the tough part is I need to make sure the handler for the websocket
            # connection has access to this context and everything is threadsafe
            ...

        r = starlette.routing.Route(path, endpoint=endpoint)
        self.routes.append(r)

    def get(self, path: str):
        def decorator(func):
            self.add_route(path, func)
            return func

        return decorator


ur = UxuRouter()

""" wrap components and props.
Return a generic html response with a ticket containing the props.

The problem is that this is dangerous because it means that the props are readable by the client.
Could easily end up leaking internal state.

What I really really want to do is share the manager object for the lifetime of the initial request and
websocket request.
"""


@ur.get("/{text:str}")
def read_root(text: str):
    return h(
        "main",
        {},
        [
            h("h1", {}, "Hello World"),
            h("p", {}, ["The content was: ", text]),
        ],
    )


router = APIRouter()


class TicketClaims(BaseModel):
    exp: datetime
    iat: datetime
    jti: str
    aud: str
    iss: str
    sub: Optional[UUID]


class Settings(BaseSettings):
    jwt_expires: timedelta = Field(default=timedelta(hours=2))
    jwt_algorithm: str = Field(default="HS256")
    jwt_secret: SecretStr

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


router.mount("/static", StaticFiles(packages=["uxu.static"]), name="uxu scripts")


@router.websocket("/uxu")
async def uxu_socket(websocket: WebSocket):
    await websocket.accept()
    transport = WebsocketTransport(websocket)
    with UxuSession(transport) as server:
        await server.serve_forever()


def DummyComponent(path):
    return h(
        "div", {}, [h("h1", {}, "Hello World"), h("p", {}, ["The url was: ", path])]
    )


class PeerInfo(BaseModel):
    name: str
    version: str


class UxuInitParams(BaseModel):
    clientInfo: PeerInfo
    url: str
    ticket: str
    # some kind of DOM checksum


class UxuInitResponse(BaseModel):
    serverInfo: PeerInfo


class UxuSession(RpcServer):
    def __init__(self, transport: Transport):
        super().__init__(transport, init_mode=InitializationMode.ExpectInit)
        self.manager = Manager()

    @rpc_method("initialize")
    async def on_initialize(self, params: UxuInitParams):
        cfg = Settings()  # type: ignore
        if not isinstance(params, UxuInitParams):
            raise invalid_params()
        claims = jwt.decode(
            params.ticket,
            key=cfg.jwt_secret.get_secret_value(),
            algorithms=[cfg.jwt_algorithm],
        )
        claims = TicketClaims.parse_obj(claims)
        # [todo] validate audience
        # [todo] validate jti to ensure no replays
        # [todo] url should be validated? ticket should be per-route

        # [todo] we should route to different things.
        spec = h(DummyComponent, path=params.url)

        self.manager.initialize(spec)

        self.patch_task = asyncio.create_task(self.patcher_loop())

        return UxuInitResponse(
            serverInfo=PeerInfo(name="uxu-server", version=__version__)
        )

    @rpc_method("render")
    def on_render(self, params):
        """Request to render."""
        return self.manager.render()

    @rpc_method("event")
    def handle_event(self, params: EventArgs):
        return self.manager.handle_event(params)

    async def patcher_loop(self):
        while True:
            try:
                patches = await self.manager.wait_patches()
                if len(patches) > 0:
                    result = await self.request("patch", patches)
                    # [todo] send encoded patches
                    logger.debug(f"patcher_loop patched: {result}")
            except asyncio.CancelledError:
                logger.debug("patcher_loop: cancelled")
                break
            except Exception as e:
                # [todo] this is for debugging only
                logger.exception("patcher_loop threw an exception")
                break
        logger.debug("patcher_loop: done")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patch_task.cancel()
        self.manager.dispose()
