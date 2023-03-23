from uxu.html import Html, h, div
from uxu.manager import EventArgs, Manager
from uxu.patch import InvalidatePatch, Patch, ReplaceElementPatch
from uxu.fiber import useState
from uxu.rendering import (
    EventHandler,
    RenderedElement,
    RenderedFragment,
    Rendering,
    RootRendering,
    RenderedText,
)
from itertools import product
import pprint

from uxu.patch import ModifyChildrenPatch

from hypothesis import given
import hypothesis.strategies as st

import pytest


def patch_triple(
    s1: Html, s2: Html
) -> tuple[RootRendering, list[Patch], RootRendering]:
    with Manager() as m:
        m.initialize(s1)
        r1 = m.render()
        assert isinstance(r1, RootRendering)
        m.update(s2)
        ps = m.get_patches()
        r2 = m.render()
        assert isinstance(r2, RootRendering)
        return r1, ps, r2


def render_prop(s1: Html, s2: Html):
    r1, ps, r2 = patch_triple(s1, s2)
    r3 = r1
    for p in ps:
        assert not isinstance(p, InvalidatePatch)
        r3 = p.apply(r3)
    assert r2 == r3


def hydrate_prop(s: Html):
    with Manager(is_static=True) as m:
        m.initialize(s)
        r1 = m.render()
        assert isinstance(r1, RootRendering)
    with Manager(is_static=False) as m:
        m.hydrate(r1, s)
        r2 = m.render()
        assert isinstance(r2, RootRendering)
    assert r1 == r2


examples = [
    "hello",
    "",
    "world",
    div("hello", "world"),
    div("world", "hello"),
    div("hello"),
    div("hello", "hello"),
    div(),
    div(div("cheese", key="x"), div("world", key="y")),
    div(div("cheese", key="x"), div("world", key="x")),
    h("span", {}, key="x"),
    h("div", {}, key="x"),
    div(x="y"),
    div(x="z"),
    div(x="y", y="x"),
]


def test_examples():
    for e1, e2 in product(examples, examples):
        render_prop(e1, e2)


def test_hydrate_examples():
    for s in examples:
        hydrate_prop(s)


def test_patch_1():
    e1 = div("x", "y")
    e2 = div("y", "x")
    r1, ps, r2 = patch_triple(e1, e2)
    assert len(ps) == 1
    p = ps[0]
    assert isinstance(p, ModifyChildrenPatch)
    assert p.reorder.l1_len == p.reorder.l2_len


@pytest.mark.asyncio
async def test_button():
    def C(x: str):
        y = useState("1")
        return h("button", {"onclick": lambda _: y.set("2")}, [x, y.current])

    with Manager() as m:
        m.initialize(h(C, "x"))
        r1 = m.render()
        assert isinstance(r1, RootRendering)
        assert len(r1.children) == 1
        f = r1.children[0]
        assert isinstance(f, RenderedFragment)
        b = f.children[0]
        assert isinstance(b, RenderedElement)
        assert b.tag == "button"
        assert "onclick" in b.attrs
        assert [x.value for x in b.children if isinstance(x, RenderedText)] == [
            "x",
            "1",
        ]
        eh = b.attrs["onclick"]
        assert isinstance(eh, EventHandler)
        ea = EventArgs(eh.handler_id, name="onclick", params={})
        m.handle_event(ea)
        ps = await m.wait_patches()
        assert len(ps) == 1
        p = ps[0]
        assert isinstance(p, ModifyChildrenPatch)
        r2 = m.render()
        f = r2.children[0]
        assert isinstance(f, RenderedFragment)
        b = f.children[0]
        assert isinstance(b, RenderedElement)
        assert b.tag == "button"
        assert "onclick" in b.attrs
        eh2 = b.attrs["onclick"]
        assert eh == eh2
        assert [x.value for x in b.children if isinstance(x, RenderedText)] == [
            "x",
            "2",
        ]
        m.handle_event(ea)
