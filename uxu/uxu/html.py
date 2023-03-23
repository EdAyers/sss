from functools import partial, singledispatch
from typing import Any, Literal, Optional, Sequence, TypeVar, Union, overload

from .textnode import TextNodeSpec
from .vdom import Html, NormSpec, normalise_html
from .element import ElementSpec
from .fiber import Component, FiberSpec
from .util import ParamSpec

P = ParamSpec("P")

"""
[todo] use the https://github.com/Knio/dominate libarary.
or at least mimick it.
 """


@overload
def h(tag: str, attrs: dict, *children: Html, key: Optional[str] = None) -> ElementSpec:
    ...


@overload
def h(tag: Component[P], *args: P.args, **kwargs: P.kwargs) -> FiberSpec:
    ...


def h(tag, attrs, *children: Html, key=None, **kwargs) -> Union[ElementSpec, FiberSpec]:  # type: ignore
    if type(tag) == str:
        if not isinstance(attrs, dict):
            raise TypeError("attrs must be a dict")
        if len(kwargs) > 0:
            raise ValueError("kwargs are not supported for tags")
        if key is not None:
            attrs["key"] = key
        all_children = normalise_html(list(children))
        return ElementSpec(tag=tag, attrs=attrs, children=all_children)
    elif callable(tag):
        args = [attrs, *children]
        return FiberSpec(component=tag, props_args=args, key=key, props_kwargs=kwargs)
    else:
        raise TypeError(f"unrecognised tag: {tag}")


# [todo] type-safe html elements.
# [todo] type-safe CSS inline styles.


def alias(tag):
    def core(*children, **attrs):
        return h(tag, attrs, *children)

    return core


def img(src: str, alt="image"):
    return h("img", dict(src=src, alt=alt))


div = alias("div")
p = alias("p")
