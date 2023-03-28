import asyncio
from dataclasses import dataclass, field
import inspect
import logging
from typing import Any, Callable, Optional
from .html import Html
from .vdom import (
    Id,
    Vdom,
    dispose,
    fresh_id,
    hydrate_lists,
    reconcile_lists,
    create,
    render,
    normalise_html,
    set_vdom_context,
)
from .patch import ModifyChildrenPatch, Patch
from .rendering import RootRendering, Rendering
from miniscutil.asyncio_helpers import MessageQueue

logger = logging.getLogger("uxu")


@dataclass
class EventArgs:
    handler_id: str
    name: str
    params: Optional[Any]


class Manager:
    is_static: bool
    id: Id

    event_table: dict[str, Callable]
    event_tasks: set[asyncio.Task]
    root: list[Vdom]
    pending_patches: MessageQueue[Patch]

    def __init__(self, spec: Optional[Html] = None, is_static: bool = False):
        self.is_static = is_static

        self.event_table = {}
        self.event_tasks = set()
        self.pending_patches = MessageQueue()
        if spec is not None:
            self.initialize(spec)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_initialized:
            self.dispose()

    def initialize(self, html: Html):
        if self.is_initialized:
            raise RuntimeError(
                "Manager is already initialized, can't initialize. Use update() instead."
            )
        with set_vdom_context(self):
            self.id = fresh_id()
            spec = normalise_html(html)
            self.root = create(spec)

    def hydrate(self, old: RootRendering, html: Html):
        if self.is_initialized:
            raise RuntimeError("Manager is already initialized, can't hydrate")
        with set_vdom_context(self):
            self.id = old.id
            spec = normalise_html(html)
            new_root, reorder = hydrate_lists(old.children, spec)
            self.root = new_root
            self._patch(
                ModifyChildrenPatch(
                    element_id=self.id,
                    reorder=reorder,
                )
            )

    def update(self, html: Html):
        if not self.is_initialized:
            return self.initialize(html)
        with set_vdom_context(self):
            spec = normalise_html(html)
            new_root, reorder = reconcile_lists(self.root, spec)
            self.root = new_root
            self._patch(
                ModifyChildrenPatch(
                    element_id=self.id,
                    reorder=reorder,
                )
            )

    @property
    def is_initialized(self):
        return hasattr(self, "root")

    def render(self) -> RootRendering:
        self.pending_patches.clear()
        children = render(self.root)
        return RootRendering(
            id=self.id,
            children=children,
        )

    def dispose(self):
        if not self.is_initialized:
            logger.debug("Manager disposed twice or before initialization")
            return
        for t in self.event_tasks:
            t.cancel()
        with set_vdom_context(self):
            dispose(self.root)
            delattr(self, "root")
        self.pending_patches.clear()
        self.event_table.clear()

    def _patch(self, patch: Patch):
        if patch.is_empty:
            return
        self.pending_patches.push(patch)

    def _register_event(self, k: str, handler: Callable):
        if self.is_static:
            return
        assert k not in self.event_table
        self.event_table[k] = handler

    def _unregister_event(self, k: str):
        if self.is_static:
            return
        assert k in self.event_table
        del self.event_table[k]

    def handle_event(self, params: EventArgs):
        if self.is_static:
            raise RuntimeError("cannot handle events in static mode")
        with set_vdom_context(self):
            assert isinstance(params, EventArgs)
            logger.debug(f"handling {params.handler_id}")
            k = params.handler_id
            if k not in self.event_table:
                logger.debug(f"No handler for {params.handler_id}")
                return
            assert k in self.event_table
            handler = self.event_table[k]

            r = handler(params.params)
            if inspect.iscoroutine(r):
                et = asyncio.create_task(r)
                self.event_tasks.add(et)
                et.add_done_callback(self.event_tasks.discard)
                # note that we don't cancel event tasks if the handler gets replaced.

        # event handler will call code to invalidate components.
        # [todo] trigger a re-render

    async def wait_patches(self):
        if self.is_static:
            raise RuntimeError("cannot handle patching loop in static mode")
        return await self.pending_patches.wait_pop_many()

    def get_patches(self):
        if self.is_static:
            return []
        else:
            return self.pending_patches.pop_all()


def render_static(html: Html) -> RootRendering:
    with Manager(spec=html, is_static=True) as m:
        r = m.render()
        return r
