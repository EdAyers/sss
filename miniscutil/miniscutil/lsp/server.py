import logging
from .types import (
    InitializeParams,
    InitializeResult,
    PeerInfo,
    ServerCapabilities,
    ApplyWorkspaceEditParams,
)
from ..rpc import InitializationMode, rpc_method
from ..rpc.extrarpc import ExtraRpc

""" Implementation of an LSP server """

logger = logging.getLogger("LSP")


class LspServer(ExtraRpc):
    capabilities: ServerCapabilities

    def __init__(self, transport):
        self.capabilities = ServerCapabilities()
        super().__init__(transport, init_mode=InitializationMode.ExpectInit)

    @rpc_method("initialize")
    async def on_initialize(self, params: InitializeParams) -> InitializeResult:
        # [todo] inject lsp capabilities here.
        return InitializeResult(
            serverInfo=PeerInfo(name=self.name, version=None),
            capabilities=self.capabilities,
        )

    @rpc_method("initialized")
    async def on_client_initialized(self, params):
        logger.info("client initialized")

    async def apply_workspace_edit(self, params: ApplyWorkspaceEditParams):
        return await self.request("workspace/applyEdit", params)
