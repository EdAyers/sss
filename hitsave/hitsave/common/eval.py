from abc import abstractmethod
from datetime import datetime
from enum import Enum
import json
import pickle
import inspect
from typing import Any, Iterator, Literal, NewType, Optional, ClassVar
from dataclasses import asdict, dataclass, field, fields
from uuid import UUID

from blobular import AbstractBlobStore, BlobInfo
from miniscutil import dict_diff
from dxd import Schema, col, transaction, Table
from pydantic import BaseModel

from .binding import Binding, BindingRecord
from .symbol import Symbol
from .digest import Digest


class EvalLookupError(LookupError):
    pass


@dataclass
class CodeChangedError(EvalLookupError):
    old_deps: Optional[dict[str, str]] = field(default=None)
    new_deps: Optional[dict[str, str]] = field(default=None)

    @property
    def reason(self) -> str:
        if self.old_deps is not None and self.new_deps is not None:
            return f"code changed"
            # [todo] refactor this.
            # diffs = pp_diffs(self.old_deps, self.new_deps)
            # return f"code changed: \n{diffs}"
        else:
            return "function hash has changed"

    def __str__(self) -> str:
        return self.reason


@dataclass
class PollEvalResult:
    value: Any
    origin: Literal["local", "cloud"]


@dataclass
class Arg:
    """Representation of the value of an argument to a function."""

    name: str
    value_digest: Digest
    is_default: bool
    kind: inspect._ParameterKind
    docs: Optional[str]


@dataclass
class EvalKey:
    """An EvalKey is a unique identifier for an evaluation"""

    symbol: Symbol
    bindings_digest: Digest
    closure_digest: Digest
    args_digest: Digest

    def __str__(self):
        return f"{str(self.symbol)}|{self.bindings_digest}|{self.closure_digest}|{self.args_digest}"

    def __hash__(self):
        return hash(str(self))

    def dict(self):
        return {f.name: getattr(self, f.name) for f in fields(self)}


class EvalStatus(Enum):
    started = 0
    rejected = 1
    """ The evaluation finished with an error or in some other incomplete state. (eg generator that never finished) """
    resolved = 2
    """ The evaluation finished successfully. """


@dataclass
class Session(Schema):
    id: UUID = col(primary=True)
    hostname: str = col()
    started: datetime = col()


@dataclass
class Eval(Schema):
    id: UUID = col(primary=True)
    symbol: Symbol = col()
    bindings_digest: Digest = col()
    closure_digest: Digest = col()
    args_digest: Digest = col()
    session_id: UUID = col()
    """ The execution session that ran the eval. """
    dependencies: dict[Symbol, Digest] = col(
        encoding="json"
    )  # [todo] implement as a 'join dict'
    """ Map from symbols to digests of Bindings. """
    is_experiment: bool = col(default=False)
    """ An eval marked as experiment will not be deleted by caching system. """
    result_digest: Optional[Digest] = col(default=None)
    result_length: Optional[int] = col(default=None)
    user_id: Optional[UUID] = col(foreign_key=True, default=None)
    """ The user that created the eval. """
    status: EvalStatus = col(default=EvalStatus.started)
    elapsed_process_time: Optional[int] = col(default=None)
    start_time: datetime = col(default_factory=datetime.now)
    args: Optional[list[Arg]] = col(encoding="json", default=None)

    @property
    def key(self) -> EvalKey:
        return EvalKey(
            symbol=self.symbol,
            bindings_digest=self.bindings_digest,
            closure_digest=self.closure_digest,
            args_digest=self.args_digest,
        )


class AbstractEvalStore:
    @abstractmethod
    def delete(self, id: UUID) -> None:
        """Deletes eval for a given id."""
        raise NotImplementedError()

    @abstractmethod
    def get_symbols(self) -> Iterator[Symbol]:
        """Return all distinct symbols that are in the evalstore."""
        raise NotImplementedError()
