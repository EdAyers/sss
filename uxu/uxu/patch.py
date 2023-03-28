from dataclasses import dataclass, field, is_dataclass, replace
from typing import Any, Union

from .rendering import Rendering, RenderedElement, RenderedAttrVal, RootRendering
from .listdiff import Reorder


from abc import ABC, abstractmethod

"""
[todo] ABC
[todo] serialisation
[todo] patch application
"""


class Patch(ABC):
    kind: str

    @property
    def is_empty(self):
        return False

    @abstractmethod
    def apply(self, root: RootRendering):
        """Apply the given patch to the given rendering.

        raises:
            RuntimeError: When trying to apply an InvalidatePatch.
        """
        raise NotImplementedError()


@dataclass
class InvalidatePatch(Patch):
    """Used to trigger a re-render of entire tree. Avoid."""

    kind: str = field(default="invalidate")

    def apply(self, root: Rendering):
        raise RuntimeError("Can't apply an invalidation patch, please rerender.")


@dataclass
class ModifyAttributesPatch(Patch):
    remove: list[str]
    add: dict[str, RenderedAttrVal]
    element_id: str
    kind: str = field(default="modify-attrs")

    @property
    def is_empty(self) -> bool:
        return len(self.remove) == 0 and len(self.add) == 0

    def apply(self, root: RootRendering):
        def visit(r: Rendering):
            assert isinstance(r, RenderedElement), "invalid id"
            attrs = {k: v for k, v in r.attrs.items() if k not in self.remove}
            attrs.update(self.add)
            return replace(r, attrs=attrs)

        return root.lens_id(self.element_id, visit)


@dataclass
class ModifyChildrenPatch(Patch):
    element_id: str
    reorder: Reorder[Rendering]
    kind: str = field(default="modify-children")

    @property
    def is_empty(self) -> bool:
        return self.reorder.is_identity

    def apply(self, root: RootRendering):
        def visit(r: Rendering):
            cs: list[Rendering] = getattr(r, "children")
            cs2 = self.reorder.apply(cs)
            assert is_dataclass(r)
            return replace(r, children=cs2)

        return root.lens_id(self.element_id, visit)


@dataclass
class ReplaceElementPatch(Patch):
    element_id: str
    new_element: Rendering  # output of render
    kind: str = field(default="replace-element")

    def apply(self, root: Rendering):
        return root.lens_id(self.element_id, lambda r: self.new_element)


@dataclass
class ReplaceRootPatch(Patch):
    root : RootRendering
    kind: str = field(default="replace-root")

    def apply(self, root: RootRendering):
        return self.root
