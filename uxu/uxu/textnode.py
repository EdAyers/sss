from dataclasses import dataclass
from typing import ClassVar

from .rendering import RenderedText, Rendering
from .patch import InvalidatePatch, ReplaceElementPatch
from .vdom import Id, NormSpec, Vdom, patch, fresh_id, to_html


@dataclass
class TextNodeSpec(NormSpec):
    value: str

    def create(self):
        return TextNode(key=self.key, id=fresh_id(), value=self.value)

    def hydrate(self, r: Rendering) -> "TextNode":
        if isinstance(r, RenderedText) and r.key == self.key:
            return TextNode(key=self.key, id=r.id, value=r.value)
        else:
            new_element = self.create()
            patch(
                ReplaceElementPatch(element_id=r.id, new_element=new_element.render())
            )
            return new_element

    @property
    def key(self):
        return hash(("text", self.value))

    def __str__(self):
        return f"TextNodeSpec({self.value})"


@dataclass
class TextNode(Vdom):
    spec_type: ClassVar = TextNodeSpec
    key: int
    id: Id
    value: str

    def __str__(self) -> str:
        return self.value

    def dispose(self):
        return

    def render(self) -> Rendering:
        return RenderedText(self.value, self.id)

    def reconcile(self, new_spec: TextNodeSpec) -> "TextNode":
        if isinstance(new_spec, TextNodeSpec) and new_spec.key == self.key:
            return self
        else:
            self.dispose()
            x = new_spec.create()
            patch(ReplaceElementPatch(element_id=self.id, new_element=x.render()))
            return x


@to_html.register(str)
def to_html_str(value: str):
    return TextNodeSpec(value)
