from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Generic, Literal, Optional, TypeAlias, TypeVar, Union
from typing import Optional as opt
import urllib.parse

T = TypeVar("T")

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

    @classmethod
    def of_pos(cls, pos: Position):
        return cls(pos, pos)


@dataclass
class TextDocumentParams:
    textDocument: TextDocumentIdentifier

    def __fspath__(self):
        return self.textDocument.__fspath__()


@dataclass
class TextDocumentPositionParams:
    """A text document identifier and a position within that document."""

    textDocument: TextDocumentIdentifier
    position: Position


@dataclass
class DidSaveTextDocumentParams:
    textDocument: TextDocumentIdentifier
    text: opt[str] = field(default=None)
    """ The content when saved. Depends on the includeText value when the save notification was requested. """


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
class ClientWorkspaceCapabilities:
    applyEdit: opt[bool] = field(default=None)
    # workspaceEdit: opt[WorkspaceEditClientCapabilities] = field(default=None)


@dataclass
class ClientCapabilities:
    textDocument: opt[TextDocumentClientCapabilities] = field(default=None)
    workspace: opt[ClientWorkspaceCapabilities] = field(default=None)
    # notebookDocument
    # window
    # general
    # experimental


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
class SaveOptions:
    includeText: Optional[bool] = field(default=None)


@dataclass
class TextDocumentSyncOptions:
    openClose: Optional[bool] = field(default=None)
    change: Optional[TextDocumentSyncKind] = field(default=None)
    willChange: Optional[bool] = field(default=None)
    willSaveWaitUntil: Optional[bool] = field(default=None)
    save: Optional[Union[bool, SaveOptions]] = field(default=None)


@dataclass
class TextDocumentItem:
    uri: DocumentUri
    languageId: str
    version: int
    text: str
    """ Content of the opened text document. """

    def __fspath__(self):
        return self.uri


@dataclass
class DidOpenTextDocumentParams:
    textDocument: TextDocumentItem


@dataclass
class DocumentFilter:
    language: Optional[str]
    """ A language id, like `typescript`. """
    scheme: Optional[str]
    """ A Uri scheme, like `file` or `untitled`. """
    pattern: Optional[str]
    """ A glob pattern, like `*.{ts,js}`. """


DocumentSelector: TypeAlias = list[DocumentFilter]


@dataclass
class TextDocumentRegistrationOptions:
    documentSelector: Optional[DocumentSelector] = field(default=None)


@dataclass
class TextDocumentChangeRegistrationOptions:
    syncKind: TextDocumentSyncKind


@dataclass
class TextDocumentContentChangeEvent:
    range: Optional[Range]
    rangeLength: Optional[int]
    text: str
    """ The new text of the whole document, or the replacement text for the range if the range field is provided. """


@dataclass
class DidCloseTextDocumentParams:
    textDocument: TextDocumentIdentifier


class TextDocumentSaveReason(Enum):
    Manual = 1
    AfterDelay = 2
    FocusOut = 3


@dataclass
class WillSaveTextDocumentParams:
    textDocument: TextDocumentIdentifier
    reason: TextDocumentSaveReason


@dataclass
class DidChangeTextDocumentParams:
    """
    https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/#didChangeTextDocumentParams
    """

    textDocument: TextDocumentIdentifier
    contentChanges: list[TextDocumentContentChangeEvent]


@dataclass
class CodeLensOptions:
    resolveProvider: Optional[bool] = field(default=None)


@dataclass
class ServerWorkspaceFileOperationCapabilities:
    # didCreate
    # willCreate
    # didRename
    # willRename
    # didDelete
    # willDelete
    pass


@dataclass
class ServerWorkspaceCapabilities:
    # workspaceFolders
    # fileOperations
    pass


@dataclass
class ServerCapabilities:
    positionEncoding: Optional[PositionEncodingKind] = field(default=None)
    textDocumentSync: Optional[TextDocumentSyncOptions] = field(default=None)
    codeLensProvider: Optional[CodeLensOptions] = field(default=None)
    # notebookDocumentSync
    # completionProvider
    # hoverProvider
    # signatureHelpProvider
    # declarationProvider
    # definitionProvider
    # typeDefinitionProvider
    # implementationProvider
    # referencesProvider
    # documentHighlightProvider
    # documentSymbolProvider
    # codeActionProvider
    # documentLinkProvider
    # colorProvider
    # documentFormattingProvider
    # documentRangeFormattingProvider
    # documentOnTypeFormattingProvider
    # renameProvider
    # foldingRangeProvider
    # executeCommandProvider
    # selectionRangeProvider
    # linkedEditingRangeProvider
    # callHierarchyProvider
    # semanticTokensProvider
    # monikerProvider
    # typeHierarchyProvider
    # inlineValueProvider
    # inlayHintProvider
    # diagnosticProvider
    # workspaceSymbolProvider
    # workspace
    # experimental : Any = field(default = None)


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


ProgressToken: TypeAlias = str


@dataclass
class WorkDoneProgressParams:
    workDoneToken: Optional[ProgressToken] = field(default=None)
    """ Optional token that a server can use to report work done progress. """


@dataclass
class WorkDoneProgressBegin:
    title: str
    """ Examples: 'Indexing', 'Linking dependencies'. """
    kind: Literal["begin"] = field(default="begin")
    cancellable: Optional[bool] = field(default=None)
    message: Optional[str] = field(default=None)
    percentage: Optional[int] = field(default=None)


@dataclass
class WorkDoneProgressReport:
    kind: Literal["report"] = field(default="report")
    cancellable: Optional[bool] = field(default=None)
    message: Optional[str] = field(default=None)
    percentage: Optional[int] = field(default=None)
    """ Percentage of work done (100 = 100%). """


@dataclass
class WorkDoneProgressEnd:
    kind: Literal["end"] = field(default="end")
    message: Optional[str] = field(default=None)


WorkDoneProgressValue = Union[
    WorkDoneProgressBegin, WorkDoneProgressReport, WorkDoneProgressEnd
]

ChangeAnnotationIdentifier: TypeAlias = str


@dataclass
class TextEdit:
    range: Range
    newText: str
    annotationId: Optional[ChangeAnnotationIdentifier] = field(default=None)


@dataclass
class CreateFileOptions:
    overwrite: Optional[bool] = field(default=None)
    ignoreIfExists: Optional[bool] = field(default=None)


@dataclass
class CreateFile:
    kind: Literal["create"]
    uri: DocumentUri
    options: Optional[CreateFileOptions] = field(default=None)
    annotationId: Optional[ChangeAnnotationIdentifier] = field(default=None)


@dataclass
class RenameFileOptions:
    overwrite: Optional[bool] = field(default=None)
    ignoreIfExists: Optional[bool] = field(default=None)


@dataclass
class RenameFile:
    kind: Literal["rename"]
    oldUri: DocumentUri
    newUri: DocumentUri
    options: Optional[RenameFileOptions] = field(default=None)
    annotationId: Optional[ChangeAnnotationIdentifier] = field(default=None)


@dataclass
class DeleteFileOptions:
    recursive: Optional[bool] = field(default=None)
    ignoreIfNotExists: Optional[bool] = field(default=None)


@dataclass
class DeleteFile:
    kind: Literal["delete"]
    uri: DocumentUri
    options: Optional[DeleteFileOptions] = field(default=None)
    annotationId: Optional[ChangeAnnotationIdentifier] = field(default=None)


@dataclass
class TextDocumentEdit:
    textDocument: TextDocumentIdentifier
    edits: list[TextEdit]


@dataclass
class ChangeAnnotation:
    label: str
    needsConfirmation: Optional[bool] = field(default=None)
    description: Optional[str] = field(default=None)


@dataclass
class WorkspaceEdit:
    changes: Optional[dict[DocumentUri, list[TextEdit]]] = field(default=None)
    documentChanges: Optional[
        list[Union[TextDocumentEdit, CreateFile, RenameFile, DeleteFile]]
    ] = field(default=None)
    changeAnnotations: Optional[
        dict[ChangeAnnotationIdentifier, ChangeAnnotation]
    ] = field(default=None)


@dataclass
class ApplyWorkspaceEditParams:
    edit: WorkspaceEdit
    label: Optional[str] = field(default=None)
