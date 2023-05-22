from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Optional
import urllib.parse

DocumentUri = str

# [todo] use TypedDict or BaseModel instead?

""" This file contains a Python implementation of the Language Server Protocol datatypes.

It is not an exhaustive set of types, I add to it as needed.

All docstrings are copied verbatim from the specification.

https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification
"""


def path_of_uri(uri: DocumentUri):
    x = urllib.parse.urlparse(uri)
    assert x.netloc == ""
    assert x.scheme == "file"
    return Path(x.path)


@dataclass
class TextDocumentIdentifier:
    """
    References:
    - https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#textDocumentIdentifier

    """

    uri: str
    version: Optional[int]
    """
    The version number of a document will increase after each change, including undo/redo. The number doesn't need to be consecutive.
    The server can send `null` to indicate that the version is known and the content on disk is the master (as specified with document content ownership).
    """

    def __fspath__(self):
        # https://docs.python.org/3/library/os.html#os.PathLike.__fspath__
        return str(path_of_uri(self.uri))


@dataclass
class Position:
    line: int
    character: int


@dataclass
class Range:
    start: Position
    end: Position

    @classmethod
    def mk(cls, l0: int, c0: int, l1: int, c1: int):
        return cls(Position(l0, c0), Position(l1, c1))


@dataclass
class TextDocumentContentChangeEvent:
    range: Optional[Range]
    rangeLength: Optional[int]
    text: str


@dataclass
class TextDocumentParams:
    textDocument: TextDocumentIdentifier

    def __fspath__(self):
        return self.textDocument.__fspath__()


@dataclass
class TextDocumentPositionParams:
    textDocument: TextDocumentIdentifier
    position: Position


@dataclass
class DidChangeTextDocumentParams:
    """
    https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#didChangeTextDocumentParams
    """

    textDocument: TextDocumentIdentifier
    contentChanges: list[TextDocumentContentChangeEvent]


@dataclass
class PeerInfo:
    name: str
    version: Optional[str]


@dataclass
class WorkspaceFolder:
    uri: DocumentUri
    name: str


@dataclass
class TextDocumentSyncClientCapabilities:
    dynamicRegistration: Optional[bool]
    willSave: Optional[bool]
    willSaveWaitUntil: Optional[bool]
    didSave: Optional[bool]


@dataclass
class CodeLensClientCapabilities:
    dynamicRegistration: Optional[bool]


@dataclass
class TextDocumentClientCapabilities:
    synchronization: Optional[TextDocumentSyncClientCapabilities]
    codeLens: Optional[CodeLensClientCapabilities]


@dataclass
class ClientCapabilities:
    textDocument: Optional[TextDocumentClientCapabilities]


@dataclass
class InitializeParams:
    processId: Optional[int] = field(default=None)
    locale: Optional[str] = field(default=None)
    workspaceFolders: Optional[list[WorkspaceFolder]] = field(default=None)
    clientInfo: Optional[PeerInfo] = field(default=None)
    initializationOptions: Optional[Any] = field(default=None)
    capabilities: Optional[ClientCapabilities] = field(default=None)
    trace: Optional[Literal["off", "messages", "verbose"]] = field(default=None)


class PositionEncodingKind(Enum):
    UTF8 = "utf-8"
    UTF16 = "utf-16"
    UTF32 = "utf-32"


class TextDocumentSyncKind(Enum):
    none = 0
    full = 1
    incremental = 2


@dataclass
class TextDocumentSyncOptions:
    openClose: Optional[bool] = field(default=None)
    change: Optional[TextDocumentSyncKind] = field(default=None)


@dataclass
class CodeLensOptions:
    resolveProvider: Optional[bool] = field(default=None)


@dataclass
class ServerCapabilities:
    positionEncoding: Optional[PositionEncodingKind] = field(default=None)
    textDocumentSync: Optional[TextDocumentSyncOptions] = field(default=None)
    codeLensProvider: Optional[CodeLensOptions] = field(default=None)


@dataclass
class InitializeResult:
    capabilities: Optional[ServerCapabilities] = field(default=None)
    serverInfo: Optional[PeerInfo] = field(default=None)


@dataclass
class CodeLensParams:
    textDocument: TextDocumentIdentifier


@dataclass
class Command:
    """Represents a reference to a command.

    Provides a title which will be used to represent a command in the UI.
    Commands are identified by a string identifier.
    The recommended way to handle commands is to implement their execution on the server side if the client and server provides the corresponding capabilities.

    https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#command
    """

    title: str
    """ Title of the command, like `save`. """
    command: str
    """ The identifier of the actual command handler. """
    arguments: Optional[list[Any]] = field(default=None)


@dataclass
class CodeLens:
    """A code lens represents a command that should be shown along with
    source text, like the number of references, a way to run tests, etc.

    A code lens is _unresolved_ when no command is associated to it. For
    performance reasons the creation of a code lens and resolving should be done
    in two stages.

    https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#codeLens
    """

    range: Range
    command: Optional[Command] = field(default=None)
    data: Optional[dict] = field(default=None)


CodeLensResponse = Optional[list[CodeLens]]
