from dataclasses import dataclass, field
from typing import Any, Union

from .rendering import Rendering
from .listdiff import Reorder

"""
[todo] ABC
[todo] serialisation
[todo] patch application
"""


@dataclass
class InvalidatePatch:
    """Used to trigger a re-render of entire tree. Avoid."""

    kind: str = field(default="invalidate")

    @property
    def is_empty(self) -> bool:
        return False


@dataclass
class ModifyAttributesPatch:
    remove: list[str]
    add: dict[str, Any]
    element_id: str
    kind: str = field(default="modify-attrs")

    @property
    def is_empty(self) -> bool:
        return len(self.remove) == 0 and len(self.add) == 0


@dataclass
class ModifyChildrenPatch:
    element_id: str
    reorder: Reorder[Rendering]
    kind: str = field(default="modify-children")

    @property
    def is_empty(self) -> bool:
        return self.reorder.is_identity



@dataclass
class ReplaceElementPatch:
    element_id: str
    new_element: Rendering  # output of render
    kind: str = field(default="replace-element")

    @property
    def is_empty(self) -> bool:
        return False


@dataclass
class ReplaceRootPatch:
    items: list[Rendering]  # output of render
    kind: str = field(default="replace-root")

    @property
    def is_empty(self) -> bool:
        return False


# [todo] RPC-encoding for patches
Patch = Union[
    ModifyAttributesPatch,
    ModifyChildrenPatch,
    ReplaceRootPatch,
    ReplaceElementPatch,
    InvalidatePatch,
]
