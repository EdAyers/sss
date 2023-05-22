from .transport import *
from typing import Awaitable
from enum import Enum
from starlette.websockets import WebSocket, WebSocketDisconnect


class StarletteWebsocketTransport(Transport):
    def __init__(self, socket: WebSocket):
        # [todo] what is the type of socket?
        self.socket = socket

    async def recv(self):
        try:
            message = await self.socket.receive()
            t = message["type"]
            if t == "websocket.disconnect":
                code = message["code"]
                reason = message["reason"]
                if code in [1000, 1001]:
                    raise TransportClosedOK(reason)
                else:
                    raise TransportClosedError(reason)
            elif t == "websocket.receive":
                if "text" in message:
                    return message["text"].encode("utf-8")
                elif "bytes" in message:
                    return message["bytes"]
                else:
                    raise TransportError("Unknown message type")
            else:
                raise TransportError(f"Unknown message type {t}")
        except WebSocketDisconnect as e:
            if e.code in [1000, 1001]:
                raise TransportClosedOK(e.reason)
            else:
                raise TransportClosedError(e.reason) from e
        except Exception as e:
            raise TransportError() from e

    async def send(self, data: bytes):
        await self.socket.send_bytes(data)
