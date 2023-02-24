import asyncio
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Generic,
    Optional,
    ParamSpec,
    Protocol,
    TypeVar,
    Union,
    overload,
)
from .rendering import RenderedFragment, Rendering
from .patch import ModifyChildrenPatch
from .vdom import (
    Html,
    NormSpec,
    Vdom,
    VdomContext,
    create,
    dispose,
    fresh_id,
    normalise_html,
    patch,
    vdom_context,
    reconcile_lists,
)
import logging

logger = logging.getLogger("uxu")


S = TypeVar("S")

SetterFn = Callable[[Union[S, Callable[[S], S]]], None]


class NoNeedToRerender(Exception):
    pass


class AbstractHook(Protocol):
    def reconcile(self, new_hook) -> "AbstractHook":
        ...

    def dispose(self) -> None:
        ...

    def initialize(self) -> None:
        ...


class StateHook(Generic[S]):
    state: S
    fiber: Optional["Fiber"]

    def __init__(self, init: S, fiber: "Fiber"):
        self.state = init
        self.fiber = fiber

    def __str__(self):
        return f"<{type(self).__name__} {type(self.state)}>"

    def reconcile(self, new_hook: "StateHook") -> "StateHook":
        assert type(self) == type(new_hook)
        return self

    def dispose(self):
        self.fiber = None

    @property
    def current(self) -> S:
        """Alias for `self.state`"""
        return self.state

    def invalidate(self):
        if self.fiber is not None:
            self.fiber.invalidate()

    def modify(self, fn: Callable[[S], S]) -> None:
        old_state = self.state
        self.state = fn(old_state)
        # [todo] add equality check
        self.invalidate()
        logger.debug(f"{str(self.fiber)}: {old_state} -> {self.state}")
        return

    def initialize(self) -> None:
        return

    def set(self, item: S) -> None:
        return self.modify(lambda _: item)


class EffectHook:
    def __init__(self, callback, deps):
        self.task = None
        self.callback = callback
        self.deps = deps

    def __str__(self):
        return f"<{type(self).__name__}>"

    def dispose(self):
        if self.task is not None:
            self.task.cancel()
            self.task = None

    def initialize(self):
        self.evaluate()

    def evaluate(self):
        callback = self.callback
        assert callable(callback)
        self.dispose()
        if asyncio.iscoroutinefunction(callback):
            self.task = asyncio.create_task(callback())
        else:
            callback()

    def reconcile(self, new_hook: "EffectHook"):
        assert type(new_hook) == type(self)

        def update():
            self.deps = new_hook.deps
            self.callback = new_hook.callback
            self.evaluate()

        if self.deps is None and new_hook.deps is None:
            update()
        elif len(self.deps) != len(new_hook.deps):
            update()
        else:
            for old_dep, new_dep in zip(self.deps, new_hook.deps):
                if old_dep != new_dep:
                    logger.debug(f"Dep changed {old_dep} -> {new_dep}")
                    update()
                    break
        return self


P = ParamSpec("P")


class Component(Protocol[P]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Html:
        ...


@dataclass
class FiberSpec(Generic[P], NormSpec):
    component: Component[P]
    props_args: list
    props_kwargs: dict
    key: Optional[str] = field(default=None)

    @property
    def name(self):
        return getattr(self.component, "__name__", "unknown")

    def create(self):
        return Fiber.create(self)

    def __str__(self):
        return self.name


H = TypeVar("H", bound="AbstractHook")


class Fiber(Vdom):
    """Like React fibers."""

    spec_type: ClassVar = FiberSpec

    component: "Component"
    props_args: list
    props_kwargs: dict

    hooks: list[AbstractHook]
    hook_idx: int
    rendered: list[Vdom]
    invalidated_event: asyncio.Event
    update_loop_task: asyncio.Task

    @property
    def name(self) -> str:
        return getattr(self.component, "__name__")

    def __str__(self) -> str:
        return f"<{self.name} {self.id}>"

    def __init__(self, spec: "FiberSpec"):
        self.id = fresh_id()
        self.key = spec.key  # type: ignore
        self.invalidated_event = asyncio.Event()
        component = spec.component
        if not hasattr(component, "__name__"):
            logger.warning(f"Please name component {component}.")
        self.component = component
        self.props_args = spec.props_args
        self.props_kwargs = spec.props_kwargs
        assert not hasattr(self, "rendered") and not hasattr(
            self, "hooks"
        ), "already created"
        self.hooks = []
        self.hook_idx = 0
        t = fiber_context.set(self)
        s = self.component(*self.props_args, **self.props_kwargs)
        self.rendered = create(normalise_html(s))
        fiber_context.reset(t)
        self.update_loop_task = asyncio.create_task(self.update_loop())

    async def update_loop(self):
        while True:
            await self.invalidated_event.wait()
            logger.debug(f"{str(self)} rerendering.")
            self.invalidated_event.clear()
            try:
                self.reconcile_core()
            except Exception as e:
                logger.exception("failure in update loop")

    def dispose(self):
        # [todo] can I just use GC?
        assert hasattr(self, "hooks")
        assert hasattr(self, "rendered"), "fiber is not rendered"
        dispose(self.rendered)
        for hook in reversed(self.hooks):
            hook.dispose()
        self.update_loop_task.cancel()

    def invalidate(self):
        """Called when a hook's callback is invoked, means that a re-render must occur."""
        logger.debug(f"{str(self)} invalidated.")
        self.invalidated_event.set()

    def reconcile_hook(self, hook: H) -> H:
        if self.hook_idx >= len(self.hooks):
            # initialisation case
            self.hooks.append(hook)
            self.hook_idx += 1
            hook.initialize()
            return hook

        old_hook = self.hooks[self.hook_idx]
        if type(old_hook) != type(hook):
            logger.error(
                f"{self} {self.hook_idx}th hook changed type from {str(old_hook)} to {str(hook)}"
            )
            old_hook.dispose()
            self.hooks[self.hook_idx] = hook
            hook.initialize()
            self.hook_idx += 1
            return hook
        else:
            self.hook_idx += 1
            return old_hook.reconcile(hook)  # type: ignore

    def reconcile_core(self):
        self.invalidated_event.clear()
        t = fiber_context.set(self)
        self.hook_idx = 0
        try:
            spec = self.component(*self.props_args, **self.props_kwargs)
        except NoNeedToRerender:
            assert hasattr(self, "rendered")
            logger.debug(f"{str(self)} Skipping re-render")
            return
        except Exception as e:
            logger.exception(f"{self} error while rendering")
            # [todo] inject a message into DOM here, or set the border colour.
            return
        finally:
            fiber_context.reset(t)
        spec = normalise_html(spec)
        children, reorder = reconcile_lists(self.rendered, spec)
        self.rendered = children
        l = self.hook_idx + 1
        old_hooks = self.hooks[l:]
        self.hooks = self.hooks[:l]
        for hook in reversed(old_hooks):
            hook.dispose()
        patch(
            ModifyChildrenPatch(
                element_id=self.id,
                remove_these=reorder.remove_these,
                then_insert_these=reorder.then_insert_these,
                children_length_start=reorder.l1_len,
            )
        )
        return

    def reconcile(self, new_spec: "FiberSpec") -> "Fiber":
        assert isinstance(new_spec, FiberSpec)
        assert hasattr(self, "hooks") and hasattr(self, "rendered"), "not created"
        # if the identity of the component function has changed that
        # means we should rerender.
        if new_spec.name != self.name or self.component is not new_spec.component:
            self.dispose()
            new_fiber = Fiber(new_spec)
            return new_fiber
        # [todo] check whether the props have changed here
        if (
            self.props_args == new_spec.props_args
            and self.props_kwargs == new_spec.props_kwargs
            and not self.invalidated_event.is_set()
        ):
            logger.debug(f"{str(self)} has unchanged props. Skipping re-render.")
            return self
        self.props_args = new_spec.props_args
        self.props_kwargs = new_spec.props_kwargs
        self.reconcile_core()
        return self

    def render(self) -> Rendering:
        return RenderedFragment(
            id=self.id, children=[x.render() for x in self.rendered]
        )

    @classmethod
    def create(cls, spec: FiberSpec):
        return cls(spec)


fiber_context: ContextVar[Fiber] = ContextVar("fiber_context")


class StateVar(Protocol[S]):
    @property
    def current(self) -> S:
        ...

    def set(self, item: S):
        ...

    def modify(self, fn: Callable[[S], S]):
        ...

    def invalidate(self):
        ...


def useState(init: S) -> StateVar[S]:
    ctx = fiber_context.get(None)
    if ctx is None:
        raise RuntimeError("useState can only be used inside a component function.")
    return ctx.reconcile_hook(StateHook(init, ctx))


def useEffect(callback: Callable[[], Coroutine[Any, Any, None]], deps=None):
    ctx = fiber_context.get()
    ctx.reconcile_hook(EffectHook(callback, deps))
