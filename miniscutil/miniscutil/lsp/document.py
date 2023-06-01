import bisect
import contextlib
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import Enum
import functools
import itertools
from typing import Iterable, Optional, Union

try:
    from typing import TypeAlias, TypeVar
except:
    from typing_extensions import TypeAlias, TypeVar
from miniscutil.misc import set_ctx


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
    def of_offset(cls, offset: int):
        return document_context.get().offset_to_position(offset)

    def to_offset(self):
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

    def __le__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) <= (other.line, other.character)

    def __eq__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) == (other.line, other.character)

    def __lt__(self, other: "Position"):
        assert isinstance(other, Position)
        return (self.line, self.character) < (other.line, other.character)


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

    def to_offsets(self):
        return self.start.to_offset(), self.end.to_offset()

    def __contains__(self, pos: Position):
        return self.start <= pos <= self.end

    @classmethod
    def union(cls, items: Iterable["Range"]):
        a, b = itertools.tee(items)
        start = min(r.start for r in a)
        end = max(r.end for r in b)
        return cls(start, end)

    @classmethod
    def intersection(cls, items: Iterable["Range"]):
        a, b = itertools.tee(items)
        start = max(r.start for r in a)
        end = min(r.end for r in b)
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
        offset += len(line.encode(enc)[: position.character * word_length].decode(enc))
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
        char = len(line[:line_offset].encode(enc))
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


@dataclass
class TextDocumentItem(DocumentContext):
    uri: DocumentUri
    languageId: str
    version: int

    def __fspath__(self):
        return self.uri
