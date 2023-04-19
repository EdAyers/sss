from abc import ABC, abstractmethod
from typing import Awaitable, Protocol

"""
Abstract definition of Transport for RPC.

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


class Transport(ABC):
    """Abstract datagram transport. Data can be sent and recieved in finite-length bytestring messages."""

    @abstractmethod
    def recv(self) -> Awaitable[bytes]:
        """Wait to recieve the a message.

        Raises:
          TransportClosedOK: if the transport is closed properly.
          TransportClosedError: if the transport is closed with an error.
          TransportError: if the transport recieved an invalid or corrupted message or some other error where the connection is not closed.
        """
        ...

    @abstractmethod
    def send(self, data: bytes) -> Awaitable[None]:
        ...
