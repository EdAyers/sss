from .transport import *
from typing import Awaitable

import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK


class WebsocketTransport(Transport):
    def __init__(self, socket):
        # [todo] what is the type of socket?
        self.socket = socket

    async def recv(self) -> Awaitable[bytes]:
        try:
            return await self.socket.recv()
        except ConnectionClosedError as e:
            raise TransportClosedError() from e
        except ConnectionClosedOK as e:
            raise TransportClosedOK() from e
        except Exception as e:
            raise TransportError() from e

    async def send(self, data: bytes):
        await self.socket.send(data)
