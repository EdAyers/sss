from dataclasses import dataclass, field, replace
from typing import Any, Callable, Union

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


@dataclass
class RenderedText:
    value: str
    id: Any
    kind: str = field(default="text")


@dataclass
class RenderedElement:
    id: Any
    tag: str
    attrs: dict[str, RenderedAttrVal]
    children: list["Rendering"]
    kind: str = field(default="element")


@dataclass
class RenderedFragment:
    id: Any
    children: list["Rendering"]
    kind: str = field(default="fragment")


@dataclass
class RenderedWidget:
    """This is used to hook into JavaScript code."""

    id: Any
    name: str
    props: Any
    kind: str = field(default="widget")


Rendering = Union[RenderedElement, RenderedText, RenderedFragment]


def iter_event_handlers(x: "Rendering"):
    if isinstance(x, RenderedElement):
        for name, v in x.attrs.items():
            if isinstance(v, EventHandler):
                yield name, v
    children = getattr(x, "children", [])
    for child in children:
        yield from iter_event_handlers(child)


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
