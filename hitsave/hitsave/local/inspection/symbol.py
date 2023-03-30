from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import importlib
from importlib.machinery import ModuleSpec
import importlib.util
import importlib.metadata
import builtins
import inspect
from pathlib import Path
import sys
import symtable as st
from symtable import SymbolTable
import ast
import pprint
import os.path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)
from functools import cached_property

from hitsave.util.graph import DirectedGraph
from hitsave.local.settings import Settings, __version__
from functools import cache
from hitsave.local.console import internal_error, logger
from miniscutil.ofdict import JsonLike
from hitsave.common import Symbol

# [todo] merge with changes from xyz/client:main


def get_origin(module_name: str) -> Optional[str]:
    """Returns a string with the module's file path as a string.

    If the module is not found, throw an error.
    If the module is a builtin, returns 'built-in'.
    """
    m = sys.modules.get(module_name)
    f = getattr(m, "__file__", None)
    if f is not None:
        return f
    spec = get_module_spec(module_name)
    return getattr(spec, "origin", None)


@cache
def get_source(module_name: str) -> Optional[str]:
    """Returns the sourcefile for the given module."""
    o = get_origin(module_name)
    if o is None:
        internal_error("No source for", module_name)
        return None
    # [todo] assert it's a python file with ast etc.
    with open(o, "rt") as f:
        return f.read()


@cache
def symtable_of_module_name(module_name: str) -> Optional[st.SymbolTable]:
    o = get_origin(module_name)
    src = get_source(module_name)
    if o is None or src is None:
        internal_error("No source or origin for", module_name)
        return None
    return st.symtable(src, o, "exec")


@cache
def get_module_spec(module_name: str) -> ModuleSpec:
    assert isinstance(module_name, str)
    m = sys.modules.get(module_name)
    spec = None
    if m is not None:
        assert hasattr(m, "__spec__"), "all modules should have __spec__?"
        spec = getattr(m, "__spec__")
    if spec is None:
        # [todo] this can raise a value error if `module_name = '__main__'` and we are degubbing.
        spec = importlib.util.find_spec(module_name)
    assert spec is not None
    return spec


@cache
def module_name_of_file(path: Union[str, Path]) -> Optional[str]:
    """Given a file location, gives a non-relative module name.

    This is supposed to be the inverse of the
    default [module finder](https://docs.python.org/3/glossary.html#term-finder)
    """
    path = str(Path(path).absolute())
    # reference: https://stackoverflow.com/questions/897792/where-is-pythons-sys-path-initialized-from
    ps = [p for p in sys.path]
    ps.reverse()
    for p in ps:
        if p == "":
            continue
        if os.path.commonpath([p, path]) == p:
            r = os.path.relpath(path, p)
            r, ext = os.path.splitext(r)
            assert ext == ".py"
            r = r.split(os.path.sep)
            r = ".".join(r)
            return r
    logger.error(f"Can't find module {path}. Did you `pip install -e .` your package?")
    return None


def compute_main_module_name():
    """Returns the name of the __main__ module.

    In Python, the convention is that the file that Python is invoked with is given a
    special `__main__` module name. However this is bad for HitSave, because we
    need to be able to identify the function's symbol independently of how the symbol's
    file was invoked. This function is a best-guess attempt to find the module name that
    would be used to import the file that is currently running as `__main__`
    """
    m = sys.modules.get("__main__")
    assert m is not None
    if not hasattr(m, "__file__"):
        # this happens in an interactive session.
        # [todo] support for using hitsave in a python repl is not implemented yet.
        return "interactive"
    mf = getattr(m, "__file__")
    assert isinstance(mf, str)
    module = module_name_of_file(mf)
    assert module is not None
    assert module != "__main__"
    return module


def get_bound_object(symb: Symbol):
    """Returns the best-guess python object associated with the symbol."""
    m = importlib.import_module(symb.module_name)
    if symb.decl_name is None:
        return m
    if "." in symb.decl_name:
        xs = symb.decl_name.split(".")
        for x in xs:
            m = getattr(m, x)
        return m
    d = getattr(m, symb.decl_name)
    return d


def resolve_module(module_name: str):
    """Returns the module that this vertex lives in.

    This does not cause the module to be loaded.
    """
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = get_module_spec(module_name)
    if spec is None:
        raise ModuleNotFoundError(name=module_name)
    return importlib.util.module_from_spec(spec)


def object_is_resolvable(obj) -> bool:
    """Returns whether we can deduce a Symbol for the given object.

    This means that the object has __qualname__ and __module__ attributes.
    This happens when the object is a class, function or module.
    """
    try:
        gbo = get_bound_object(symbol_of_object(obj))
        return gbo is obj
    except Exception:
        return False


def symbol_of_object(o) -> Symbol:
    """Create a Symbol from a python object by trying to inspect what the Symbol and parent module are."""
    if inspect.ismodule(o):
        module_name = o.__name__
        decl_name = None
        return Symbol(o.__name__)
    elif not hasattr(o, "__qualname__") or not hasattr(o, "__module__"):
        raise ValueError(
            f"Object {o} does not have a __qualname__ or __module__ attribute."
        )
    else:
        module_name = o.__module__
        assert module_name is not None, f"Module for {o} not found."
        decl_name = o.__qualname__
    if module_name == "__main__":
        module_name = compute_main_module_name()
    return Symbol(module_name, decl_name)


def get_st_symbol(symb: Symbol) -> Optional[st.Symbol]:
    """Return the SymbolTable Symbol for this Symbol."""
    if symb.decl_name is None:
        return None

    st = symtable_of_module_name(symb.module_name)
    if st is None:
        internal_error(f"Failed to find symbol table for", symb)
        return None
    try:
        if "." in symb.decl_name:
            parts = symb.decl_name.split(".")
            for part in parts[:-1]:
                s = st.lookup(part)
                if s.is_namespace():
                    st = s.get_namespace()
            s = st.lookup(parts[-1])
            return s
            # [todo] test this
        return st.lookup(symb.decl_name)
    except KeyError as e:
        logger.debug(f"Failed to find symbol {str(symb)}: {e}")
        return None


def is_namespace(symb: Symbol) -> bool:
    """Returns true if the symbol is a SymbolTable namespace, which means that there is some
    internal structure; eg a function, a class, a module."""
    if symb.decl_name is None:
        # all modules are namespaces
        return True
    sts = get_st_symbol(symb)
    if sts is None:
        return False
    return sts.is_namespace()


def is_module(symb: Symbol):
    return symb.decl_name is None or inspect.ismodule(get_bound_object(symb))


def is_import(symb: Symbol) -> bool:
    """Returns true if the symbol was declared from an `import` statement."""
    if symb.decl_name is None:
        return False
    sts = get_st_symbol(symb)
    if sts is None:
        return False
    return sts.is_imported()
