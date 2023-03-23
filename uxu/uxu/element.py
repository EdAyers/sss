from dataclasses import dataclass
from typing import ClassVar, Union
from .rendering import EventHandler, RenderedAttrVal
from miniscutil import dict_diff
import logging
from .patch import ModifyAttributesPatch, ModifyChildrenPatch, ReplaceElementPatch
from .vdom import (
    Id,
    NormSpec,
    Vdom,
    VdomContext,
    create,
    dispose,
    fresh_id,
    hydrate_lists,
    patch,
    reconcile_lists,
    vdom_context,
)
from .rendering import Rendering, RenderedElement

logger = logging.getLogger("reactor")


@dataclass
class ElementSpec(NormSpec):
    tag: str
    attrs: dict
    children: list[NormSpec]

    @property
    def key(self):
        return hash(("element", self.tag, self.attrs.get("key", None)))

    def __str__(self) -> str:
        return f"<{self.tag}/>"

    def create(self) -> "Element":
        return Element.create(self)

    def hydrate(self, r):
        return Element.hydrate(r, self)


@dataclass
class Element(Vdom):
    spec_type: ClassVar = ElementSpec
    tag: str
    attrs: dict[str, RenderedAttrVal]
    children: list[Vdom]
    id: Id
    key: int

    def __str__(self) -> str:
        return f"<{self.tag} {self.id}>"

    @classmethod
    def create(cls, spec: ElementSpec):
        id = fresh_id()
        attrs = {}
        for k, v in spec.attrs.items():
            if callable(v):
                handler_id = f"{id}/{k}"
                vdom_context.get()._register_event(handler_id, v)
                v = EventHandler(handler_id)
            attrs[k] = v
        elt = cls(
            spec.tag, attrs=attrs, children=create(spec.children), id=id, key=spec.key
        )
        return elt

    @classmethod
    def hydrate(cls, r: Rendering, spec: ElementSpec) -> "Element":
        if not isinstance(r, RenderedElement) or r.tag != spec.tag or r.key != spec.key:
            logger.debug(f"not a matching element, replacing")
            new_element = cls.create(spec)
            patch(ReplaceElementPatch(r.id, new_element.render()))
            return new_element
        else:
            id = r.id
            new_attrs = {}
            # [todo] abstract this attrs, registering loop.
            for k, v in spec.attrs.items():
                if callable(v):
                    handler_id = f"{id}/{k}"
                    vdom_context.get()._register_event(handler_id, v)
                    v = EventHandler(handler_id)
                new_attrs[k] = v
            diff = dict_diff(r.attrs, new_attrs)
            remove = list(diff.rm)
            add = {k: new_attrs[k] for k in diff.add}
            mod = {k: v2 for k, (v1, v2) in diff.mod.items() if v1 != v2}
            patch(
                ModifyAttributesPatch(remove=remove, add={**add, **mod}, element_id=id)
            )
            children, reorder = hydrate_lists(r.children, spec.children)
            patch(ModifyChildrenPatch(id, reorder))
            elt = cls(
                spec.tag, attrs=new_attrs, children=children, id=r.id, key=spec.key
            )
            return elt

    def dispose(self):
        # delete references to event handlers.
        for k, v in self.attrs.items():
            if isinstance(v, EventHandler):
                vdom_context.get()._unregister_event(v.handler_id)
        dispose(self.children)

    def render(self) -> Rendering:
        return RenderedElement(
            id=self.id,
            tag=self.tag,
            key=self.key,
            attrs=self.attrs,
            children=[c.render() for c in self.children],
        )

    def reconcile_attrs(self, new_attrs_spec: dict) -> dict:
        for k, v in self.attrs.items():
            if isinstance(v, EventHandler):
                vdom_context.get()._unregister_event(v.handler_id)
        new_attrs = {}
        for k, v in new_attrs_spec.items():
            if callable(v):
                handler_id = f"{self.id}/{k}"
                vdom_context.get()._register_event(handler_id, v)
                v = EventHandler(handler_id)
            new_attrs[k] = v
        diff = dict_diff(self.attrs, new_attrs)
        remove = list(diff.rm)
        # [todo] need to unregister event handlers in diff.rm?
        add = {k: new_attrs[k] for k in diff.add}
        mod = {k: v2 for k, (v1, v2) in diff.mod.items()}
        patch(
            ModifyAttributesPatch(remove=remove, add={**add, **mod}, element_id=self.id)
        )
        self.attrs = new_attrs
        return self.attrs

    def reconcile(self, new_spec: ElementSpec) -> "Element":
        assert isinstance(new_spec, ElementSpec)
        logger.debug(f"reconcile {str(self)} ← {str(new_spec)}")
        if (self.key != new_spec.key) or (self.tag != new_spec.tag):
            self.dispose()
            v = new_spec.create()
            logger.debug(f"replacing {str(self)} → {str(v)}")
            patch(ReplaceElementPatch(self.id, v.render()))
            return v
        self.reconcile_attrs(new_spec.attrs)
        children, r = reconcile_lists(self.children, new_spec.children)
        # [todo] apply expander to r here.
        patch(ModifyChildrenPatch(element_id=self.id, reorder=r))
        self.tag = new_spec.tag
        self.children = children
        return self
