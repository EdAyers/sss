from abc import ABC, abstractmethod
from dataclasses import dataclass, field, is_dataclass, replace
from typing import Any, Callable, Union
from textwrap import indent
from html import escape

""" A rendering is the thing that we actually send over the wire to Javascript. """


@dataclass
class EventHandler:
    handler_id: Any

    def __eq__(self, other):
        if not isinstance(other, EventHandler):
            return False
        return self.handler_id == other.handler_id

    def __todict__(self):
        return {"__handler__": self.handler_id}

    @classmethod
    def __ofdict__(cls, d: dict):
        return EventHandler(d["__handler__"])


RenderedAttrVal = Union[EventHandler, str, dict]


# [todo] abstract class
class Rendering(ABC):
    id: str
    kind: str
    key: Any

    @abstractmethod
    def static(self):
        raise NotImplementedError()

    def map_children(self, f):
        if hasattr(self, "children"):
            return self.with_children(list(map(f, getattr(self, "children"))))
        return self

    def with_children(self, children):
        assert hasattr(self, "children")
        assert is_dataclass(self)
        return replace(self, children=children)

    def get_children(self):
        return getattr(self, "children", [])

    def get_ids(self, acc=[]):
        acc.append(self.id)
        for c in self.get_children():
            c.get_ids(acc)
        assert len(set(acc)) == len(acc), "non-unique id"
        return acc

    def lens_id(self, id, f):
        if self.id == id:
            return f(self)
        cs = getattr(self, "children", None)
        if cs is None:
            raise LookupError()
        for i, c in enumerate(cs):
            try:
                r = c.lens_id(id, f)
            except LookupError:
                continue
            if not isinstance(r, Rendering):
                raise TypeError("function must return rendering")
            cs = cs.copy()
            cs[i] = r
            return replace(self, children=cs)  # type: ignore
        raise LookupError()


@dataclass
class RootRendering(Rendering):
    """Rendering at the root of the uxu mount point."""

    id: Any
    children: list[Rendering]
    key: Any = field(default=None)

    def static(self):
        raise RuntimeError("RootRendering can't be statically rendered")


@dataclass
class RenderedText(Rendering):
    value: str
    id: Any
    kind: str = field(default="text")

    def static(self):
        return self.value

    @property
    def key(self):
        return hash(("text", self.value))


@dataclass
class RenderedElement(Rendering):
    id: Any
    tag: str
    attrs: dict[str, RenderedAttrVal]
    children: list["Rendering"]
    key: Any = field(default=None)
    kind: str = field(default="element")

    def static(self):
        from dominate.dom_tag import dom_tag
        import dominate.tags as tags

        def static_render_attr(k, v):
            if isinstance(v, str):
                return v
            elif isinstance(v, dict):
                assert k == "style"
                xs = "; ".join(f"{k}: {s}" for k, s in v.items())
                return f'{k}="{xs}"'
            else:
                raise TypeError()

        attrs = {
            k: static_render_attr(k, v)
            for k, v in self.attrs.items()
            if not isinstance(v, EventHandler)
        }
        attrs = {**self.attrs, "data-uxu-id": self.id}
        cls = getattr(tags, self.tag)
        children = [c.static() for c in self.children]
        elt = cls(*children, **attrs)
        assert isinstance(elt, dom_tag)
        return elt


@dataclass
class RenderedFragment(Rendering):
    id: Any
    children: list["Rendering"]
    key: Any = field(default=None)
    kind: str = field(default="fragment")

    def static(self):
        return [c.static() for c in self.children]


@dataclass
class RenderedWidget:
    """This is used to hook into JavaScript code."""

    # [todo] make rendering
    id: Any
    name: str
    props: Any
    kind: str = field(default="widget")


# [todo] put on Rendering methods
def iter_event_handlers(x: "Rendering"):
    if isinstance(x, RenderedElement):
        for name, v in x.attrs.items():
            if isinstance(v, EventHandler):
                yield name, v
    children = getattr(x, "children", [])
    for child in children:
        yield from iter_event_handlers(child)


# [todo] use methods on Rendering instead.
def map_event_handlers(modify: Callable[[EventHandler], EventHandler]):
    def rec(x: Rendering) -> Rendering:
        if isinstance(x, RenderedElement):
            attrs = {
                k: modify(v) if isinstance(v, EventHandler) else v
                for k, v in x.attrs.items()
            }
            children = list(map(rec, x.children))
            return replace(x, attrs=attrs, children=children)
        elif isinstance(x, RenderedText):
            return x
        elif isinstance(x, RenderedFragment):
            return replace(x, children=list(map(rec, x.children)))
        else:
            raise TypeError()

    return rec
