from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
from functools import cached_property
from typing import ClassVar, Optional
from uuid import UUID

from dxd import Schema, col
from miniscutil.ofdict import OfDictUnion

from .digest import Digest, digest_string
from .symbol import Symbol


class BindingKind(Enum):
    """Information about the kind of thing that the symbol is bound to."""

    fun = "FUN"
    """ A function, including class methods. """
    cls = "CLS"
    """ A class """
    val = "VAL"
    """ A value. These are hashed by their python object value. """
    imp = "IMP"
    """ Imported symbol. """
    constant = "CONSTANT"
    """ A constant, defining expression is hashed. """
    external = "EXTERNAL"
    """ External package """
    unresolved = "UNRESOLVED"
    """ Binding that we failed to resolve"""

CODE_BINDING_KINDS = [BindingKind.fun, BindingKind.cls, BindingKind.imp, BindingKind.constant, BindingKind.external]

class Binding(ABC, OfDictUnion):
    kind: BindingKind
    deps: set[Symbol]
    diffstr: str
    """ A string that can be diffed with other versions of the binding to show the user what changed. """
    digest: Digest
    """ A string to uniquely identify the binding object. Note that this doesn't have to be a hex hash if not needed. """

    def __hash__(self):
        return hash(self.digest)


@dataclass
class BindingRecord(Schema):
    kind: BindingKind
    deps: set[Symbol]
    diffstr: str
    digest: Digest = col(primary=True)
    user_id: Optional[UUID] = col(primary=True, default=None, foreign_key=True)


@dataclass
class ImportedBinding(Binding):
    symb: Symbol
    kind: ClassVar = BindingKind.imp

    @property
    def digest(self) -> Digest:
        return Digest(str(self.symb))

    @property
    def deps(self):
        return set([self.symb])

    @property
    def diffstr(self):
        # [todo] would be cool to show a diff of the line of sourcecode here.
        return str(self.symb)


@dataclass
class FnBinding(Binding):
    sourcetext: str
    """ Function source code as it appears in the source file. """
    deps: set[Symbol]
    kind: ClassVar = BindingKind.fun

    @cached_property
    def digest(self) -> str:
        return digest_string(self.sourcetext)

    @property
    def diffstr(self) -> str:
        return self.sourcetext


@dataclass
class ClassBinding(Binding):
    sourcetext: str
    code_deps: set[Symbol]
    methods: list[Symbol]
    kind: ClassVar = BindingKind.cls

    @property
    def deps(self):
        return self.code_deps.union(self.methods)

    @cached_property
    def digest(self) -> Digest:
        return digest_string(self.sourcetext)

    @property
    def diffstr(self) -> str:
        return self.sourcetext


@dataclass
class UnresolvedBinding(Binding):
    """This is a binding that we failed to resolve fully."""

    diffstr: str = field(default="??? unknown binding ???")
    deps: set[Symbol] = field(default_factory=set)
    digest: Digest = field(default=Digest("??????????"))
    kind: BindingKind = field(default=BindingKind.unresolved)


@dataclass
class ValueBinding(Binding):
    """A ValueBinding represents a global python object that the code depends on.

    In real python code, value bindings can mutate.
    However here (for now) we only save and hash the value at the point that we ingest the python function.
    We assume that if you are depending on constants that are not args, then they are not mutating.

    This is not true for certain objects such as loggers and pytorch models or dataloaders, but these should always be passed as args.

    [todo] this is not always desired, in the future, we will give the option to recompute the digest and dependencies at the point of function
    execution, so that we can detect whether a constant has changed.
    """

    digest: Digest
    deps: set[Symbol]
    diffstr: str
    kind: ClassVar = BindingKind.val


@dataclass
class ExternalBinding(Binding):
    digest: Digest
    name: str
    version: str
    kind: ClassVar = BindingKind.external

    @property
    def diffstr(self):
        return self.version

    @property
    def deps(self):
        return set()
