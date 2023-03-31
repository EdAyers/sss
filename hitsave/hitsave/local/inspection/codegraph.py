from abc import ABC, abstractmethod
from collections import ChainMap
import copyreg
from dataclasses import dataclass, field
import difflib
from enum import Enum
import importlib
from importlib.machinery import ModuleSpec
import io
from blake3 import blake3
from pickle import _Pickler, PicklingError
import importlib.util
import importlib.metadata
import tempfile
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
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
)
from functools import cached_property
from hitsave.common import *
from hitsave.local.settings import Settings
from hitsave.util.graph import DirectedGraph
from hitsave.__about__ import __version__
from functools import cache
from hitsave.local.console import (
    debug,
    internal_warning,
    logger,
    console,
    internal_error,
    pp_diff,
    user_warning,
)
from hitsave.local.inspection.symbol import (
    get_bound_object,
    get_module_spec,
    get_st_symbol,
    is_import,
    is_module,
    is_namespace,
    module_name_of_file,
    get_origin,
    get_source,
    symbol_of_object,
    symtable_of_module_name,
)


def value_binding_of_object(obj: Any) -> ValueBinding:
    if inspect.isfunction(obj):
        s = symbol_of_object(obj)
        diffstr = f"<function {str(s)}>"
    else:
        diffstr = pprint.pformat(obj)
    if len(diffstr) > 10000:
        diffstr = diffstr[:10000] + " ... [truncated]"
    h = HashingPickler()
    h.dump(obj)
    return ValueBinding(
        digest=h.digest,
        deps=h.code_dependencies,
        diffstr=diffstr,
    )


class CodeGraph:
    dg: DirectedGraph[Symbol, Any]

    def __init__(self):
        self.dg = DirectedGraph()

    def eat_obj(self, o):
        v = symbol_of_object(o)
        return self.eat(v)

    def eat(self, v: Symbol):
        if self.dg.has_vertex(v):
            # assume already explored
            return
        self.dg.add_vertex(v)
        if isinstance(v, Symbol):
            b = try_get_binding(v)
            for v2 in b.deps:
                self.eat(v2)
                self.dg.set_edge(v, v2, b)

    def get_dependencies(self, v: Symbol):
        self.eat(v)
        yield from self.dg.reachable_from(v)

    def get_dependencies_obj(self, o):
        yield from self.get_dependencies(symbol_of_object(o))

    def clear(self):
        self.dg = DirectedGraph()


@cache
def module_version(module_name: str) -> Optional[str]:
    try:
        return importlib.metadata.version(module_name)
    except:
        pass
    m = sys.modules.get(module_name)
    if m is None:
        internal_error("Module", module_name, "not loaded.")
        return None
    if hasattr(m, "__version__"):
        v = m.__version__
        if v is None:
            internal_error("Module", module_name, "has no __version__ field.")
            return None
        return v
    return None


def is_relative_import(module_name: str) -> bool:
    return module_name.startswith(".")


def head_module(module_name) -> str:
    """Given a dot-separated module name such as ``torch.nn.functional``,
    returns the 'head module' name ``torch``.

    Raises:
      NotImplementedError: if it's a relative import.
    """
    if "." in module_name:
        parts = module_name.split(".")
        head = parts[0]
        if head == "":
            raise NotImplementedError(
                f'head_module for relative imports not supported: "{module_name}"'
            )
        return head
    return module_name


def version_digest(version: str) -> Digest:
    vs = version.split(".")
    amt = ["none", "major", "minor", "patch"].index(
        Settings.current().version_sensitivity
    )
    v = ".".join(vs[:amt])
    return digest_string(v)


def _mk_extern_package_from_site_package(module_name: str) -> Binding:
    v = module_version(module_name)
    if v is not None:
        return ExternalBinding(name=module_name, version=v, digest=version_digest(v))
    elif "." in module_name:
        if is_relative_import(module_name):
            internal_error(
                "Finding the binding of relative imports is not supported yet",
                module_name,
            )
            return UnresolvedBinding()
        head = head_module(module_name)
        v = module_version(head)
        if v is not None:
            return ExternalBinding(name=head, version=v, digest=version_digest(v))
    internal_error(f"Can't find a module version for {module_name}.")
    return UnresolvedBinding()


@cache
def module_as_external_package(module_name: str) -> Optional[Binding]:
    """Looks at the module name and tries to determine whether the module should be considered as being
    external for the project.

    An ExternPackage is a leaf of the code dependency DAG, rather than exploring the source of an external package, we instead
    hash it according to the package version.
    """
    if not is_relative_import(module_name) and head_module(module_name) == "hitsave":
        # special case, hitsave is always an extern package
        v = __version__
        return ExternalBinding(name="hitsave", version=v, digest=version_digest(v))
    m = sys.modules.get(module_name)
    o = get_origin(module_name)
    if o is None:
        internal_error("Failed to find origin of", module_name)
        return None
    if "site-packages" in o:
        # [todo] there should be a canonical way to do this.
        return _mk_extern_package_from_site_package(module_name)
    if ("lib/python3" in o) or (o == "built-in"):
        v = sys.version_info
        v = f"{v.major}.{v.minor}"
        return ExternalBinding(name="__builtin__", version=v, digest=version_digest(v))
    # [todo] another case is packages that have been installed by the user using `pip install -e ...`
    # the rule should be configurable, but we treat it as an extern package iff
    # - it has to be a package (module has a __path__ attr)
    # - it has a __version__ attribute
    return None


def is_subpath(path1: str, path2: str):
    """Returns true if there is a `q` such that `path2 = path1 + q`"""
    return os.path.commonpath([path1, path2]) == path1


@cache
def _get_namespace_binding(s: Symbol) -> Binding:
    """Return a binding for the case of s being a namespace.
    This is it's own method because we can safely cache namespace bindings.

    Raises:
      ValueError: the symbol table entry of s is not a namespace.
    """
    if not is_namespace(s):
        raise ValueError(f"Symbol {str(s)} is not a namespace in the symbol table.")
    if is_module(s):
        return value_binding_of_object(get_bound_object(s))

    sts = get_st_symbol(s)
    assert sts is not None
    ns = sts.get_namespace()
    # [todo] what are some cases where there are multiple namespaces?
    if isinstance(ns, st.Function):
        deps = [
            Symbol(s.module_name, decl_name)
            for decl_name in ns.get_globals()
            if not hasattr(builtins, decl_name)
        ]
        src = getsource(s)
        if src is None:
            internal_error(f"failed to find sourcecode for", s)
            return UnresolvedBinding(deps=set(deps), kind=BindingKind.fun)
        return FnBinding(deps=set(deps), sourcetext=src)
    if isinstance(ns, st.Class):
        if s.decl_name is None:
            internal_error(s, "is a module but expecting a symbol")
            return UnresolvedBinding(kind=BindingKind.cls)
        methods = [
            Symbol(s.module_name, s.decl_name + "." + mn) for mn in ns.get_methods()
        ]
        src = getsource(s)
        if src is None:
            internal_error("failed to find sourcecode for class", s)
            return UnresolvedBinding(kind=BindingKind.cls)

        return ClassBinding(
            sourcetext=src,
            code_deps=set(),  # [todo] get field deps, get baseclass deps.
            methods=methods,
        )

    internal_error(f"Don't know how to get deps of namespace", s, ns)
    return UnresolvedBinding()


def get_binding(s: Symbol) -> Binding:
    """Returns the binding for a particular symbol.

    We make an assumption to allow us to cache bindings:
    If the symbol is in the symbol-table for the module's source file, we assume that the
    symbol is still bound to that source declaration at runtime. However this is not necessarily true,
    since some code later in the module could re-bind the name.

    This is the difference between symbols and names in python; a symbol is a compile-time binding and a
    name is a runtime binding.
    """
    p = module_as_external_package(s.module_name)
    if p is not None:
        return p

    o = get_bound_object(s)

    if is_import(s):
        if inspect.ismodule(o):
            n = getattr(o, "__name__", None)
            if n is None:
                internal_error("Module", o, "has no name.")
                return UnresolvedBinding()
            symb = Symbol(n)
            return ImportedBinding(symb=symb)
        imports = get_module_imports(s.module_name)
        if s.decl_name not in imports:
            internal_warning("Could not find", s.decl_name, "in AST for", s.module_name)
            return UnresolvedBinding()
        i = imports[s.decl_name]
        return ImportedBinding(symb=i)

    if is_namespace(s) and not is_module(s):
        # a namespace means that s is a function, class or module and contains references to symbols.
        return _get_namespace_binding(s)
    else:  # not a namespace
        return value_binding_of_object(o)


def try_get_binding(s: Symbol) -> Binding:
    """get_binding wrapped in a try."""
    try:
        return get_binding(s)
    except Exception as e:
        internal_warning("Failed to resolve", s, " due to uncaught error\n", e)
        return UnresolvedBinding()


def get_digest(s: Symbol):
    return get_binding(s).digest


@cache
def get_module_imports(module_name: str) -> Dict[str, Symbol]:
    """Returns all of the vertices that are imported from the given module name."""
    src = get_source(module_name)
    if src is None:
        internal_error("couldn't get source for", module_name)
        return {}
    t = ast.parse(src)
    r = {}

    def mk_vertex(module_name, fn_name=None) -> Symbol:
        return Symbol(module_name, fn_name)

    class V(ast.NodeVisitor):
        def visit_Import(self, node: ast.Import):
            for alias in node.names:
                asname = alias.asname or alias.name
                if asname in r:
                    user_warning(
                        "Multiple imports of", asname, "detected in", module_name
                    )
                r[asname] = mk_vertex(alias.name)

        def visit_ImportFrom(self, node: ast.ImportFrom):
            if node.module is None:
                internal_error(
                    "trouble traversing python AST of", module_name, ast.unparse(node)
                )
                return
            node_module = node.module
            if node.level > 0:
                # relative imports
                # reference: https://docs.python.org/3/reference/import.html#package-relative-imports
                level = node.level
                package_name: str = module_name
                while level > 0:
                    m = importlib.import_module(package_name)
                    assert m.__package__ is not None
                    package_name = m.__package__
                    level -= 1
                node_module = package_name + "." + node.module

            for alias in node.names:
                asname = alias.asname or alias.name
                if asname in r:
                    user_warning(
                        "Multiple imports of", asname, "detected in", node_module
                    )
                r[asname] = mk_vertex(node_module, alias.name)

    V().visit(t)
    return r


def pp_symbol(sym: st.Symbol):
    if not isinstance(sym, st.Symbol):
        raise TypeError(f"argument sym should be a {st.Symbol} but was {type(sym)}.")
    print("Symbol:", sym.get_name())
    ps = [
        "referenced",
        "imported",
        "parameter",
        "global",
        "declared_global",
        "local",
        "free",
        "assigned",
        "namespace",
    ]
    ps = ",".join([p for p in ps if getattr(sym, "is_" + p)()])
    return f"Symbol({sym.get_name()}, {ps})"


def pp_symtable(st: SymbolTable):
    """Pretty print a symbol table for debugging purposes."""
    if not isinstance(st, SymbolTable):
        raise TypeError(f"arg st should be a SymbolTable but was {type(st)}")

    def rec(st):
        s = f"SymbolTable(type={st.get_type()}, id={st.get_id()}, name={st.get_name()}, nested={st.is_nested()})"
        return (s, ", ".join(st.get_identifiers()), [rec(x) for x in st.get_children()])

    return pprint.pformat(rec(st))


@cache
def getsource(s: Symbol) -> Optional[str]:
    """Given a symbol, return the source string or None if there is no source."""
    o: Any = get_bound_object(s)
    try:
        if inspect.isfunction(o) or inspect.isclass(o):
            return inspect.getsource(o)
        elif hasattr(o, "__wrapped__") and inspect.isfunction(o.__wrapped__):
            return inspect.getsource(o.__wrapped__)
        else:
            return None
    except Exception as e:
        internal_error("getsource threw on", s, "\n", e)
        return None


opaque_types = set()
""" Set of python datatypes that should be ignored by the hashing pickler. Good candidates for this are logging functions """


def register_opaque(t: type):
    """Register the given type as being *opaque* with respect to the HitSave hashing algorithm.

    You can use this to tell HitSave that it shouldn't care about certain types of objects.
    Note that subtype checking is not done, if you inherit from a type registered as opaque, you will need to register that too.
    [todo] fix this; singledispatch has some code that does this.

    Returns: the input argument.
    """
    global opaque_types
    opaque_types.add(t)
    return t


digest_dispatch_table = {}


def register_digest_reductor(type):
    def core(f):
        digest_dispatch_table[type] = f
        return f

    return core


def _sortkey(x):
    # [note]: can't use hash because it is not stable.
    return (type(x).__name__, repr(x))


class Hasher(Protocol):
    # [todo] I think there's already an abstract thing for this.
    def update(self, x: bytes):
        ...

    def hexdigest(self) -> str:
        ...


class HashingWriter:
    def __init__(self, hasher: Hasher, outfile: Optional[IO[bytes]]):
        self.hasher = hasher
        self.outfile = outfile

    def write(self, b: bytes) -> None:
        # [todo] out-of-band
        self.hasher.update(b)
        if self.outfile is not None:
            self.outfile.write(b)


class HashingPickler(_Pickler):
    hasher: Hasher
    code_dependencies: Set[Symbol]
    outfile: Optional[IO[bytes]]

    def save(self, obj, save_persistent_id=True):
        try:
            super().save(obj, save_persistent_id)  # type: ignore
        except (PicklingError, TypeError) as pe:
            internal_warning(pe)
            self.save_pers("___UNPICKLABLE___")  # type: ignore

    def reducer_override(self, obj):
        if isinstance(obj, set):
            """[todo] there is a bug in the c-optimised python pickler code, where
            it will call the primitive dispath (for things like set, dict, int) before
            calling `reducer_override` wheras the python implementation will call ``reducer_override`` first.
            I suspect this is an optimisation, but the python and C implementations should really be the same.
            """
            # special dispensation for sets:
            # their elements need to be iterated in a canonical order.
            try:
                items = sorted(obj)
            except Exception as e:
                internal_error(f"Sorting error:", e)
                items = list(obj)
            return (set, (), None, items)

        return NotImplemented

    def __init__(self, protocol=None, outfile=None, **kwargs):
        self.hasher = blake3()  # type: ignore
        self.code_dependencies = set()
        self.outfile = outfile
        self.dispatch_table = ChainMap(digest_dispatch_table, copyreg.dispatch_table)
        super().__init__(
            HashingWriter(self.hasher, self.outfile), protocol=protocol, **kwargs
        )

    def persistent_id(self, obj):
        # Abusing the persistent_id mechanism.
        # https://docs.python.org/3/library/pickle.html#persistence-of-external-objects
        if type(obj) in opaque_types:
            return "___OPAQUE___"

        if inspect.ismodule(obj):
            s = symbol_of_object(obj)
            self.code_dependencies.add(s)
            return str(s)

        if hasattr(obj, "__module__") and hasattr(obj, "__qualname__"):
            if obj.__module__ is None:
                # this happens for some builtins, eg `_thread.RLock.aquire`
                return "__UNKNOWN__"
            # we have encountered a code-dependency.
            if obj.__name__ == "<lambda>":
                try:
                    src = inspect.getsource(obj)
                    return src
                except OSError as e:
                    internal_error(
                        "Found a sourceless lambda",
                        obj.__qualname__,
                    )
                    return obj.__qualname__

            if obj.__name__.startswith("<"):
                internal_error("Got an unrecognised", obj.__module__, obj.__name__)
                return f"{obj.__module__}:{obj.__name__}"
            s = symbol_of_object(obj)
            self.code_dependencies.add(s)
            return str(s)

        return None

    @property
    def digest(self) -> Digest:
        return Digest(self.hasher.hexdigest())


def value_digest(obj, outfile=None) -> str:
    """Compute the local digest of a given python object.

    Code-dependencies are discarded.

    Args:
      outfile: is a file_like writable object that writes the stream that is hashed. This is useful for debugging.
               Consider using ``debug_value_digest``.
    """
    h = HashingPickler(outfile=outfile)
    h.dump(obj)
    return h.digest


def debug_value_digest(obj) -> Tuple[str, str]:
    import pickletools

    with io.BytesIO() as f1:
        with io.StringIO() as f2:
            d1 = value_digest(obj, outfile=f1)
            f1.seek(0)
            pickletools.dis(f1, out=f2)
            return d1, f2.getvalue()


def print_digest_diff(obj1, obj2):
    d1, v1 = debug_value_digest(obj1)
    d2, v2 = debug_value_digest(obj2)
    if d1 == d2:
        console.print("Equal digest.")
    xs = pp_diff(v1, v2)
    console.print("\n".join(xs))
