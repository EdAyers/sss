import bisect
import contextlib
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import Enum
import functools
import itertools
from pathlib import Path
import re
from typing import Iterable, Optional, Union
from urllib.parse import urlparse

try:
    from typing import TypeAlias, TypeVar
except:
    from typing_extensions import TypeAlias, TypeVar
from miniscutil.misc import set_ctx
import logging

logger = logging.getLogger(__name__)


class PositionEncodingKind(Enum):
    UTF8 = "utf-8"
    UTF16 = "utf-16"
    UTF32 = "utf-32"


position_encoding_context: ContextVar[PositionEncodingKind] = ContextVar(
    "position_encoding_context", default=PositionEncodingKind.UTF16
)


DocumentUri: TypeAlias = str

document_context: ContextVar["DocumentContext"] = ContextVar("document_context")


@contextlib.contextmanager
def setdoc(doc: Union[str, "DocumentContext"]):
    """Lots of the cursor position logic needs to know what the document is to calculate
    utf-16 offsets and line-numbers correctly. This sets the 'document context' to the provided document.

    This lets you do things like:
    - add offsets to positions
    - apply edits to ranges
    - find the length in characters of a range.
    """
    if isinstance(doc, str):
        doc = DocumentContext(doc)
    with set_ctx(document_context, doc):
        yield doc


def cumsum(iterable):
    total = 0
    for v in iterable:
        total += v
        yield total


@dataclass
class Position:
    line: int
    character: int

    @classmethod
    def of_offset(cls, offset: int) -> "Position":
        return document_context.get().offset_to_position(offset)

    def to_offset(self) -> int:
        return document_context.get().position_to_offset(self)

    def __add__(self, offset: Union[int, tuple[int, int]]) -> "Position":
        if isinstance(offset, int):
            return document_context.get().add_position(self, offset)
        elif isinstance(offset, tuple):
            line, col = offset
            return replace(self, line=self.line + line, character=self.character + col)
        else:
            raise TypeError(
                f"unsupported operand type(s) for +: 'Position' and '{type(offset)}'"
            )

    def __sub__(self, other: "Position") -> int:
        assert isinstance(other, Position)
        return self.to_offset() - other.to_offset()

    def __le__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) <= (other.line, other.character)

    def __eq__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) == (other.line, other.character)

    def __lt__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) < (other.line, other.character)

    def __hash__(self):
        return hash((self.line, self.character))


@dataclass
class Range:
    start: Position
    end: Position

    @classmethod
    def mk(cls, l0: int, c0: int, l1: int, c1: int):
        return cls(Position(l0, c0), Position(l1, c1))

    @classmethod
    def of_pos(cls, pos: Position, length: int = 0):
        return cls(pos, pos + length)

    def to_offsets(self):
        return self.start.to_offset(), self.end.to_offset()

    def __contains__(self, pos: Position):
        return self.start <= pos <= self.end

    def __len__(self):
        """Gets the length of the range in unicode code points."""
        return self.end.to_offset() - self.start.to_offset()

    def __hash__(self):
        return hash((self.start, self.end))

    def __repr__(self):
        return f"Range.mk({self.start.line}, {self.start.character}, {self.end.line}, {self.end.character})"

    @classmethod
    def union(cls, items: Iterable["Range"]):
        items = list(items)
        if len(items) == 0:
            raise ValueError("cannot union empty list of ranges")
        start = min(r.start for r in items)
        end = max(r.end for r in items)
        return cls(start, end)

    @classmethod
    def intersection(cls, items: Iterable["Range"]):
        items = list(items)
        if len(items) == 0:
            # mathematically, the intersection of an empty set of
            # ranges is the whole number line.
            # but let's not do that.
            raise ValueError("cannot intersect empty list of ranges")
        start = max(r.start for r in items)
        end = min(r.end for r in items)
        if start > end:
            return None
        return cls(start, end)

    def __add__(self, offset: int):
        assert isinstance(offset, int)
        return Range(self.start + offset, self.end + offset)


@dataclass
class TextDocumentContentChangeEvent:
    range: Optional[Range]
    text: str
    """ The new text of the whole document, or the replacement text for the range if the range field is provided. """

    def apply(self, text: str) -> str:
        if self.range is None:
            return self.text
        else:
            with setdoc(text):
                start, end = self.range.to_offsets()
                text1 = text[:start] + self.text + text[end:]
                return text1

    def map_pos(self, pos: "Position"):
        if self.range is None:
            raise ValueError("cannot map position")
        if pos <= self.range.start:
            return pos
        if pos >= self.range.end:
            return pos + len(self.text)
        raise ValueError("cannot map position")

    def map_range(self, range: "Range"):
        return replace(
            range, start=self.map_pos(range.start), end=self.map_pos(range.end)
        )


SURROGATE_KEY_END = re.compile("[\ud800-\udbff]$", re.UNICODE)


@dataclass
class DocumentContext:
    text: str

    @property
    def line_count(self):
        """One plus the number of newlines in the document."""
        return len(self.line_offsets)

    @functools.cached_property
    def line_offsets(self):
        if self.text == "":
            return [0]
        lines = self.text.splitlines(keepends=True)
        assert len(lines) > 0
        offsets = list(cumsum(map(len, lines)))
        assert len(offsets) == len(lines)
        assert offsets[-1] == len(self.text)
        assert offsets[0] == len(lines[0])
        return offsets

    def get_line_start_offset(self, line_index: int) -> int:
        if line_index == 0:
            return 0
        if line_index >= self.line_count:
            return len(self.text)
        return self.line_offsets[line_index - 1]

    def get_line_end_offset(self, line_index: int) -> int:
        if line_index >= self.line_count:
            return len(self.text)
        return self.line_offsets[line_index]

    def get_line(self, index: int) -> str:
        return self.text[
            self.get_line_start_offset(index) : self.get_line_end_offset(index)
        ]

    @property
    def position_encoding(self):
        return position_encoding_context.get()

    def position_to_offset(self, position: Position):
        s = self.line_offsets
        if position.line >= self.line_count:
            # not a strictly valid position but map to end of string.
            return len(self.text)
        line = self.get_line(position.line)
        offset = s[position.line - 1] if position.line > 0 else 0
        assert self.position_encoding == PositionEncodingKind.UTF16
        enc = "utf-16-le"
        word_length = 2
        if SURROGATE_KEY_END.match(line) is not None:
            # caught a half of a surrogate pair.
            line = line[:-1]
        try:
            line_encoded = line.encode(enc)
        except UnicodeEncodeError:
            # this can happen if we read half a surrogate pair.
            logger.exception(
                f"line is not valid unicode, returning position of line start:\n{line}"
            )
            return offset
        line_encoded = line_encoded[: position.character * word_length]
        try:
            line_decoded = line_encoded.decode(enc)
        except UnicodeDecodeError:
            # this can happen if the line is not valid utf-16.
            # eg half a surrogate pair?
            # error for now but should be possible to recover.

            raise RuntimeError(
                f"failed to find offset for {position.line}, {position.character} \n{line}"
            )
        offset += len(line_decoded)
        return offset

    def offset_to_position(self, offset: int) -> Position:
        s = self.line_offsets
        line_idx = bisect.bisect_right(s, offset)
        if line_idx >= self.line_count:
            line_idx = self.line_count - 1
        line = self.get_line(line_idx)
        assert self.position_encoding == PositionEncodingKind.UTF16
        enc = "utf-16-le"
        word_length = 2
        acc = s[line_idx - 1] if line_idx > 0 else 0
        line_offset = offset - acc
        subline = line[:line_offset]
        if SURROGATE_KEY_END.match(subline) is not None:
            # caught half a surrogate pair
            subline = subline[:-1]
        try:
            char = len(subline.encode(enc))
        except UnicodeEncodeError as e:
            logger.error(
                f"failed to encode line, falling back to counting bytes:\n{subline}\n{e}"
            )
            char = len(subline) * 2

        assert char % word_length == 0
        char = char // 2
        return Position(line=line_idx, character=char)

    def add_position(self, position: Position, delta_offset: int) -> Position:
        return self.offset_to_position(self.position_to_offset(position) + delta_offset)

    def range_to_offsets(self, range: Range) -> tuple[int, int]:
        return (
            self.position_to_offset(range.start),
            self.position_to_offset(range.end),
        )

    # [todo] enter, exit does setdoc


def path_of_uri(uri: DocumentUri):
    x = urlparse(uri)
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
class TextDocumentItem(DocumentContext):
    uri: DocumentUri
    languageId: str
    version: int

    def __fspath__(self):
        return str(path_of_uri(self.uri))

    @property
    def id(self):
        return TextDocumentIdentifier(uri=self.uri, version=self.version)
