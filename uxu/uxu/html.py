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
def h(tag: str, *children: Html, key: Optional[str] = None, **kwargs) -> ElementSpec:
    ...


@overload
def h(tag: Component[P], *args: P.args, **kwargs: P.kwargs) -> FiberSpec:
    ...


def h(tag, *children, key=None, **kwargs) -> Union[ElementSpec, FiberSpec]:  # type: ignore
    if type(tag) == str:
        # [todo] emmet-style parsing of tags. Eg 'h1.myclass'
        attrs = kwargs
        if key is not None:
            attrs["key"] = key
        all_children = normalise_html(list(children))
        return ElementSpec(tag=tag, attrs=attrs, children=all_children)
    elif callable(tag):
        args = list(children)
        return FiberSpec(component=tag, props_args=args, key=key, props_kwargs=kwargs)
    else:
        raise TypeError(f"unrecognised tag: {tag}")


# [todo] type-safe html elements.
# [todo] type-safe CSS inline styles.


def alias(tag):
    def core(*children, **attrs):
        return h(tag, *children, **attrs)

    return core


def img(src: str, alt="image"):
    return h("img", src=src, alt=alt)


div = alias("div")
p = alias("p")
