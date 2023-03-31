from typing import Any
import hypothesis.strategies as hs
from hypothesis import given
import symtable
import pprint
import sys
import pytest
from hitsave.local.inspection import (
    CodeGraph,
    Symbol,
    ExternalBinding,
    get_binding,
    get_module_imports,
    pp_symbol,
    symtable_of_module_name,
    get_origin,
    get_module_spec,
    module_as_external_package,
    get_bound_object,
    object_is_resolvable,
    symbol_of_object,
)
from tests.strat import numbers
from miniscutil import partition, walk as wap


def h(z):
    s = symtable
    l = list
    return given


def g(y):
    return y + h(y) + cc + FOP


def f(x: float):
    return x + g(x)


cc = 100
dd = 101


def outer(aa):
    def inner():
        bb = 1
        return aa + bb + cc

    return inner


FOP = {"x": lambda x: x + x + dd}


class Hello:
    def __init__(self):
        self.x = 4
        self.y = f(10)

    def asdf(self):
        return self


def hhh():
    h = Hello()
    return h.asdf()


THIS_MODULE = f.__module__


def test_module_imports(snapshot):
    mis = get_module_imports(THIS_MODULE)
    bindings = {k: (v, get_binding(v)) for k, v in mis.items()}
    cvs = {
        k: str(v)
        for k, (v, b) in bindings.items()
        if not isinstance(b, ExternalBinding)
    }
    eps = {
        k: b.name for k, (v, b) in bindings.items() if isinstance(b, ExternalBinding)
    }
    snapshot.assert_match(pprint.pformat(cvs), "code_imports.txt")
    snapshot.assert_match(pprint.pformat(eps), "externs.txt")


def test_graph_snap(snapshot):
    gg = CodeGraph()
    ds: Any = list(gg.get_dependencies_obj(f))
    interns, externs = partition(
        lambda d: isinstance(get_binding(d), ExternalBinding), ds
    )
    ss = pprint.pformat((sorted(map(str, interns)), sorted(map(str, externs))))
    snapshot.assert_match(ss, "test_graph_snap.txt")


def test_symtable(snapshot):
    st = symtable_of_module_name(THIS_MODULE)
    assert st is not None
    s = st.lookup("outer")
    snapshot.assert_match(pp_symbol(s), "test_symtable.txt")


def test_origin_builtin():
    o = get_origin("itertools")
    assert o == "built-in"


def test_module_as_external_package():
    e1 = module_as_external_package("hitsave")
    assert isinstance(e1, ExternalBinding)
    assert e1.name == "hitsave"


# [todo] these tests are tricky because __main__ depends on how tests are entered.

# def test_main():
#     o = get_origin("__main__")
#     assert o is not None
# def test_get_module_spec_main():
#     s = get_module_spec("__main__")
#     assert s is not None
# def test_module_as_external_package2():
#     e1 = module_as_external_package("__main__")
#     assert e1 is None


def test_module_as_external_package3():
    e1 = module_as_external_package("itertools")
    assert isinstance(e1, ExternalBinding)
    assert e1.name == "__builtin__"


def resolve_object_prop(symb: Symbol):
    gbo = get_bound_object(symb)

    if object_is_resolvable(gbo):
        symb2 = symbol_of_object(gbo)
        msg = str(symb) + " -> " + str(symb2)
        # we do not have symb == symb2 because modules can re-export symbols.
        gbo2 = get_bound_object(symb2)
        assert object_is_resolvable(gbo2), msg
        assert gbo is gbo2, msg


@pytest.mark.parametrize(
    "symb",
    [
        Symbol("numpy", "array"),
        Symbol("builtins", "list"),
        Symbol("torch", "Tensor"),
        Symbol("torch.nn"),
        Symbol("torch.nn.functional"),
        # [todo] failing examples
        # Symbol('torch', 'conv2d'), # torch:_VariableFunctionsClass.conv2d
        # Symbol('torch.nn.functional', 'conv2d'), # torch:_VariableFunctionsClass.conv2d
        # Symbol('sys', '__excepthook__'), # but sys.excepthook is not sys.__excepthook__
    ],
)
def test_resolve_object(symb):
    gbo = get_bound_object(symb)
    assert object_is_resolvable(gbo)
