import asyncio
import logging
from pydantic import BaseModel
from uxu.lsp.types import PeerInfo
from uxu.manager import Manager, EventArgs
from uxu.rpc import rpc_method, RpcServer
from uxu.vdom import Html

from uxu.__about__ import __version__


class InitParams(BaseModel):
    clientInfo: PeerInfo


class InitResponse(BaseModel):
    serverInfo: PeerInfo


logger = logging.getLogger(__name__)


class UxuLocalSession(RpcServer):
    def __init__(self, spec: Html):
        self.manager = Manager(spec=spec, is_static=False)

    @rpc_method("initialize")
    async def on_initialize(self, params: InitParams):
        self.patch_task = asyncio.create_task(self.patcher_loop())
        return InitResponse(
            serverInfo=PeerInfo(name="UxuLocalSession", version=__version__)
        )

    @rpc_method("initialized")
    async def on_initialized(self, params: InitParams):
        logger.debug("UxuLocalSession initialized")

    @rpc_method("render")
    def on_render(self, params):
        return self.manager.render()

    @rpc_method("event")
    def on_event(self, params: EventArgs):
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
