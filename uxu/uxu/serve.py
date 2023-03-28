import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_urlsafe
from typing import Any, Callable, Optional, Protocol
from uuid import UUID, uuid4
import logging

from pydantic import BaseModel, BaseSettings, Field, SecretStr
import dominate
import dominate.tags as t
from jose import ExpiredSignatureError, JWTError, jwt

from starlette.responses import HTMLResponse
from starlette.requests import Request
from starlette.routing import Route, Mount, WebSocketRoute
from starlette.applications import Starlette
from starlette.staticfiles import StaticFiles
from starlette.websockets import WebSocket

from uxu.manager import EventArgs, Manager, render_static
from uxu.persistence import PersistDict
from uxu.rpc import (
    StarletteWebsocketTransport,
    RpcServer,
    InitializationMode,
    Transport,
)
from uxu.__about__ import __version__
from uxu.rpc.jsonrpc import invalid_params, rpc_method
from uxu.html import h
from uxu.rendering import RootRendering

# https://fastapi.tiangolo.com/advanced/websockets/?h=websocket

logger = logging.getLogger("uxu")


def HelloWorld(text: str):
    """Just a simple component to test things."""
    return h(
        "main",
        {},
        [
            h("h1", {}, "Hello World"),
            h("p", {}, ["The content was: ", text]),
        ],
    )


def component_of_name(name: str):
    return HelloWorld


@dataclass
class UxuSessionParams:
    id: str  # session id
    component_name: str
    path: str
    rendering: RootRendering
    params: dict[str, str] = field(default_factory=dict)


class UxuApplication:
    """
    I'm not completely sure how this should work yet, but the idea is that
    UxuApplication is a valid ASGI app that you can dump into Starlette or FastAPI or whatever.

    For now, I'm just doing dependency injection but I want to do this properly.

    """

    def __init__(self):
        self.cfg = Settings()  # type: ignore
        self.persistence = PersistDict(
            self.cfg.persistent_dict_path, T=UxuSessionParams
        )
        self.webapp = Starlette(
            debug=True,
            routes=[
                Mount(
                    "/static",
                    app=StaticFiles(packages=[("uxu", "static")]),
                    name="static",
                ),
                # https://www.starlette.io/routing#websocket-routing
                WebSocketRoute("/ws", endpoint=self.handle_websocket),
                Route("/{name:str}", self.handle),
            ],
        )

    async def __call__(self, scope, receive, send):
        return await self.webapp(scope, receive, send)

    async def handle(self, request: Request):
        cfg = self.cfg
        expires_delta = cfg.jwt_expires
        expire = datetime.utcnow() + expires_delta
        # [todo] request.url.path will contain the component name
        # [todo] get subject as id of authenticated user.
        socket_url = request.url_for("handle_websocket")
        static_url = request.url_for("static", path="uxu.js")
        session_id = uuid4().hex
        claims = TicketClaims(
            exp=expire,
            iat=datetime.utcnow(),
            jti=session_id,
            aud=str(socket_url),
            iss=str(request.url.path),
            # [todo] how to identify the client? needs to defend csrf and xss
            # one option is simply that they have to log in first, then pass user_id
            sub=None,
        )
        ticket = jwt.encode(
            claims=claims.dict(exclude_none=True),
            key=cfg.jwt_secret.get_secret_value(),
            algorithm=cfg.jwt_algorithm,
        )

        # [todo] implement uxu routing table.
        component = component_of_name(request.url.path)
        initial_html = component(request.url.path)
        rendering = render_static(initial_html)

        session_params = UxuSessionParams(
            id=session_id,
            component_name=component.__qualname__,
            path=request.url.path,
            params={},  # [todo]
            rendering=rendering,
        )
        self.persistence.set(session_id, session_params)  # [todo] make async
        # [todo] remove dep on dominate
        document: Any = dominate.document(title=f"Uxu {__version__}")
        with document.head:
            t.link(
                rel="stylesheet",
                href="https://unpkg.com/tachyons@4.12.0/css/tachyons.min.css",
            )
        with document.body:
            t.main("welcome to uxu...", id="uxu_root")
            # [todo] is this the best way to inject a secret?
            # try switching to using a cookie that the JS can't see.
            t.script(
                f"UXU_TICKET = '{ticket}'; UXU_URL = '{socket_url}';",
                type="text/javascript",
            )
            t.script(type="text/javascript", src=static_url)

        content = document.render()
        return HTMLResponse(content=content)

    async def handle_websocket(self, websocket: WebSocket):
        await websocket.accept()
        transport = StarletteWebsocketTransport(websocket)
        with UxuSession(
            transport, self.persistence, socket_url=str(websocket.url)
        ) as server:
            await server.serve_forever()
        await websocket.close()


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

    persistent_dict_path: Path = Field(default=Path("tmp/uxu_server.sqlite"))

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


class PeerInfo(BaseModel):
    name: str
    version: str


class UxuInitParams(BaseModel):
    clientInfo: PeerInfo
    url: str
    ticket: str
    # [todo] some kind of DOM checksum


class UxuInitResponse(BaseModel):
    serverInfo: PeerInfo


class UxuSession(RpcServer):
    """Handles a websocket session."""

    def __init__(
        self,
        transport: Transport,
        persistence: PersistDict[UxuSessionParams],
        socket_url: str,
    ):
        super().__init__(transport, init_mode=InitializationMode.ExpectInit)
        self.persistence = persistence
        self.socket_url = socket_url
        self.manager = Manager(is_static=False)

    @rpc_method("initialize")
    async def on_initialize(self, params: UxuInitParams):
        cfg = Settings()  # type: ignore
        if not isinstance(params, UxuInitParams):
            raise invalid_params()
        claims = jwt.decode(
            params.ticket,
            key=cfg.jwt_secret.get_secret_value(),
            algorithms=[cfg.jwt_algorithm],
            audience=self.socket_url,
        )
        claims = TicketClaims.parse_obj(claims)
        session_params: UxuSessionParams = self.persistence.pop(
            claims.jti
        )  # [todo] make async
        component = component_of_name(session_params.component_name)
        spec = component(session_params.path)

        # [todo] validate audience
        # [todo] validate jti to ensure no replays
        # [todo] url should be validated. ticket should be per-route
        # [todo] we should route to different things.

        self.manager.hydrate(session_params.rendering, spec)
        self.patch_task = asyncio.create_task(self.patcher_loop())

        return UxuInitResponse(
            serverInfo=PeerInfo(name="uxu-server", version=__version__)
        )

    @rpc_method("initialized")
    def on_initialized(self, params):
        logger.debug("client successfully initialized")
        return

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
        if hasattr(self, "patch_task"):
            self.patch_task.cancel()
        self.manager.dispose()


app = UxuApplication()

from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level="NOTSET", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
