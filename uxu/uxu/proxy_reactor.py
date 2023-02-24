import asyncio
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Protocol, ClassVar

from .rendering import RenderedText, iter_event_handlers

from .manager import EventArgs
from .patch import Patch, InvalidatePatch
from .vdom import NormSpec, Vdom, Rendering, fresh_id, vdom_context, patch


class ReactorLike(Protocol):
    async def wait_patches(self) -> list[Patch]:
        ...

    async def initialize(self):
        ...

    async def render(self) -> Rendering:
        ...

    def handle_event(self, args: EventArgs):
        ...

    def dispose(self):
        ...


@dataclass
class ReactorVdom:
    id: Any
    rlike: ReactorLike

    def render(self):
        r = getattr(
            self, "rendered", RenderedText(value="loading", id=f"{self.id}-loading")
        )
        return r

    def dispose(self):
        self.main_task.cancel()
        self.rlike.dispose()

    @classmethod
    def create(cls, rlike):
        x = cls(
            id=fresh_id(),
            rlike=rlike,
        )
        x._start()
        return x

    async def refresh(self):
        rendered = await self.rlike.render()
        old_handler_ids = getattr(self, "handler_ids", set())
        handler_ids = set()
        for name, eh in iter_event_handlers(rendered):
            handler_ids.add(eh.handler_id)
            old_handler_ids.discard(eh.handler_id)
            vdom_context.get()._register_event(
                eh.handler_id, partial(self.handle_event, eh.handler_id, name)
            )
        for ohid in old_handler_ids:
            vdom_context.get()._unregister_event(ohid)
        self.rendered = rendered
        self.handler_ids = handler_ids
        # [todo] do fancy patch forwarding
        patch(InvalidatePatch())

    def handle_event(self, handler_id, name, params):
        self.rlike.handle_event(
            EventArgs(handler_id=handler_id, name=name, params=params)
        )

    def _start(self):
        self.main_task = asyncio.create_task(self._patcher_loop())

    async def _patcher_loop(self):
        await self.rlike.initialize()
        await self.refresh()
        while True:
            patches = await self.rlike.wait_patches()
            if len(patches) > 0:
                # [todo] forward the patches here.
                await self.refresh()


class ThunkSpec(NormSpec):
    def __init__(self, thunk: Callable[[], Vdom], key=None):
        self.name = thunk.__name__
        self.key = hash(f"ThunkSpec {self.name} {key}")
        self.thunk = thunk

    def create(self):
        return ThunkVdom(wrapped=self.thunk(), name=self.name, key=self.key)

    def __str__(self):
        return f"ThunkSpec {self.name}"


@dataclass
class ThunkVdom(Vdom):
    spec_type: ClassVar = ThunkSpec
    wrapped: Vdom
    name: str
    key: Any

    def reconcile(self, spec: ThunkSpec):
        if spec.name == self.name:
            return self
        else:
            self.dispose()
            return spec.create()

    def __str__(self):
        return f"ThunkVdom {self.name}"

    def dispose(self):
        return self.wrapped.dispose()

    def render(self):
        return self.wrapped.render()
