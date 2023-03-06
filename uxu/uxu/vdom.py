from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from functools import singledispatch
from typing import (
    Any,
    Callable,
    ClassVar,
    Literal,
    Optional,
    Protocol,
    Union,
    overload,
)
import uuid
from .rendering import Rendering
from .listdiff import Reorder, diff as listdiff
from .patch import InvalidatePatch, Patch


class VdomContext(Protocol):
    def _patch(self, patch: Patch) -> None:
        ...

    def _register_event(self, k: str, handler: Callable):
        ...

    def _unregister_event(self, k: str):
        ...

    @property
    def is_static(self) -> bool:
        ...


vdom_context: ContextVar[VdomContext] = ContextVar("vdom_context")


@contextmanager
def set_vdom_context(r: VdomContext):
    t = vdom_context.set(r)
    try:
        yield r
    finally:
        vdom_context.reset(t)


def patch(patch: Patch):
    return vdom_context.get()._patch(patch)


class Vdom(ABC):
    spec_type: ClassVar[type]
    key: str

    @abstractmethod
    def __str__(self) -> str:
        ...

    @abstractmethod
    def render(self) -> Rendering:
        ...

    @abstractmethod
    def dispose(self):
        ...

    @abstractmethod
    def reconcile(self, new_spec: "NormSpec") -> "Vdom":
        ...


class NormSpec(ABC):
    key: int

    @abstractmethod
    def __str__(self):
        ...

    @abstractmethod
    def create(self) -> Vdom:
        ...

    @abstractmethod
    def hydrate(self, r: Rendering) -> Vdom:
        ...


@overload
def create(s: NormSpec) -> Vdom:
    ...


@overload
def create(s: list[NormSpec]) -> list[Vdom]:
    ...


def create(s):
    if isinstance(s, list):
        return list(map(create, s))
    elif isinstance(s, NormSpec):
        return s.create()
    else:
        raise TypeError(f"unrecognised spec {s}")


def hydrate(r: Rendering, s: NormSpec) -> Vdom:
    if isinstance(s, NormSpec) and isinstance(r, Rendering):
        return s.hydrate(r)
    else:
        raise TypeError(f"unrecognised {r}, {s}")


@overload
def render(s: Vdom) -> Rendering:
    ...


@overload
def render(s: list[Vdom]) -> list[Rendering]:
    ...


def render(s: Union[Vdom, list[Vdom]]):
    if isinstance(s, list):
        return list(map(render, s))
    elif isinstance(s, Vdom):
        return s.render()
    else:
        raise TypeError(f"unrecognised spec {s}")


def hydrate_lists(
    old: list[Rendering], new: list[NormSpec]
) -> tuple[list[Vdom], Reorder[Rendering]]:
    reorder: Reorder = listdiff([x.key for x in old], [x.key for x in new])
    for ri in reorder.deletions:
        pass
    new_vdom: list[Any] = [None] * len(new)
    for i, j in reorder.moves:
        assert new_vdom[j] is None
        new_vdom[j] = hydrate(old[i], new[j])
    for j in reorder.creations:
        assert new_vdom[j] is None
        new_vdom[j] = new[j].create()
    assert all(x is not None for x in new_vdom)
    reorder = reorder.map_inserts(lambda j, _: new_vdom[j].render())
    # [todo] abstract with reconcile_lists
    return new_vdom, reorder


def reconcile_lists(
    old: list[Vdom], new: list[NormSpec]
) -> tuple[list[Vdom], Reorder[Rendering]]:
    reorder: Reorder = listdiff([x.key for x in old], [x.key for x in new])
    for ri in reorder.deletions:
        old[ri].dispose()
    new_vdom: list[Any] = [None] * len(new)
    for i, j in reorder.moves:
        assert new_vdom[j] is None
        new_vdom[j] = reconcile(old[i], new[j])
    for j in reorder.creations:
        assert new_vdom[j] is None
        new_vdom[j] = new[j].create()
    assert all(x is not None for x in new_vdom)
    reorder = reorder.map_inserts(lambda j, _: new_vdom[j].render())
    return new_vdom, reorder


def reconcile(old: Vdom, new: NormSpec) -> Vdom:
    if isinstance(new, old.spec_type):
        return old.reconcile(new)  # type: ignore
    old.dispose()
    # [todo] add better patch... this shouldn't really happen because we match on keys.
    patch(InvalidatePatch())
    return new.create()


def dispose(v: Union[Vdom, list["Vdom"]]):
    if isinstance(v, list):
        for vv in v:
            vv.dispose()
    else:
        v.dispose()


Id = str

UUID = uuid.uuid4().hex[:4]
ID_COUNTER = 100


def fresh_id() -> Id:
    global ID_COUNTER
    ID_COUNTER += 1
    return Id(f"{UUID}-{ID_COUNTER}")


Html = Optional[Union[str, list["Html"], Literal[False], NormSpec]]


@singledispatch
def to_html(x: Any):
    raise NotImplementedError(f"Unknown {type(x).__name__}")


def normalise_html(c: Html) -> list[NormSpec]:
    def norm_list(cs) -> Any:
        for c in cs:
            if isinstance(c, str):
                yield to_html(c)
            elif isinstance(c, (list, tuple)):
                yield from norm_list(c)
            elif c is None or c is False or not c:
                continue
            elif isinstance(c, NormSpec):
                yield c
            elif hasattr(c, "__html__"):
                yield from normalise_html(c.__html__())
            else:
                yield to_html(c)

    return list(norm_list([c]))
