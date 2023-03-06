from typing import Awaitable, Protocol
import asyncio

"""
[todo] use tinyrpc?
"""


class TransportClosed(ConnectionError):
    pass


class TransportClosedError(TransportClosed):
    """Transport connection terminated with an error."""

    pass


class TransportClosedOK(TransportClosed):
    """Connection was terminated properly.

    Eg if the transport is reading from a pipe, EOF was received.
    If the transport is from a websocket, the socket closed with the proper handshake.
    """

    pass


class TransportError(Exception):
    """Transport recieved an invalid or corrupted message."""


class Transport(Protocol):
    """Abstract datagram transport. Data can be sent and recieved in finite-length bytestring messages."""

    def recv(self) -> Awaitable[bytes]:
        """Wait to recieve the a message.

        Raises:
          TransportClosedOK: if the transport is closed properly.
          TransportClosedError: if the transport is closed with an error.
          TransportError: if the transport recieved an invalid or corrupted message or some other error where the connection is not closed.
        """
        ...

    def send(self, data: bytes) -> Awaitable[None]:
        ...


async def create_pipe_streams(in_pipe, out_pipe):
    """Converts a pair of pipes into a reader/writer async pair."""
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, in_pipe)
    w_transport, w_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, out_pipe
    )
    writer = asyncio.StreamWriter(w_transport, w_protocol, reader, loop)
    return reader, writer


class AsyncStreamTransport(Transport):
    """Create a transport from a asyncio StreamReader, StreamWriter pair.

    We assume the message protocol is that described in LSP
    https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#baseProtocol

    That is, a sequence of newline delimited http-like header strings, terminated by a double newline.
    One of the headers needs to be "content-length" integer, and then that number of bytes is read from the stream.
    """

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    async def recv(self):
        """Recieves data from the stream. If EOF is reached, raises TransportClosedOK error."""
        # read the header
        header = {}
        while True:
            line = await self.reader.readline()
            if line == b"":
                assert self.reader.at_eof()
                if len(header) == 0:
                    raise TransportClosedOK("end of stream")
                else:
                    raise TransportClosedError(f"unexpected end of stream")
            line = line.decode().rstrip()
            if line == "":
                break
            k, v = line.split(":", 1)
            header[k.lower()] = v
        content_length = header.get("content-length")
        if content_length is None:
            raise TransportError("invalid datagram: no content-length in header")
        content_length = int(content_length)
        # read the body
        try:
            data = await self.reader.readexactly(content_length)
            return data
        except asyncio.IncompleteReadError as e:
            raise TransportClosedError("unexpected end of stream") from e

    async def send(self, data: bytes, header={}):
        header["Content-Length"] = len(data)
        header = "".join(f"{k}:{v}\r\n" for k, v in header.items())
        header += "\r\n"
        self.writer.write(header.encode())
        self.writer.write(data)


class WebsocketTransport(Transport):
    def __init__(self, socket):
        # [todo] what is the type of socket?
        self.socket = socket

    async def recv(self) -> Awaitable[bytes]:
        from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

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
