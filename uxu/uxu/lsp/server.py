import logging
from .types import InitializeParams, InitializeResult, PeerInfo, ServerCapabilities
from uxu.rpc import RpcServer, InitializationMode, rpc_method

""" Implementation of an LSP server """

logger = logging.getLogger("uxu.lsp")


class LspServer(RpcServer):
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
        logger.info("Client initialized")
