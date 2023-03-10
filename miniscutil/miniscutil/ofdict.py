from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from functools import singledispatch
import json
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    NewType,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)
import logging
from .dispatch import classdispatch
from .type_util import as_list, as_optional, is_optional

try:
    from typing import TypeGuard
except ImportError:
    from typing_extensions import TypeGuard

T = TypeVar("T")
logger = logging.getLogger(__name__)

JsonLike = Optional[Union[dict, list, str, bool, int, float]]
JsonKey = Optional[Union[str, int, float, bool]]


def is_json_key(key: Any) -> TypeGuard[JsonKey]:
    return isinstance(key, (str, int, float, bool, type(None)))


@classdispatch
def ofdict(A: Type[T], a: JsonLike) -> T:
    """Converts an ``a`` to an instance of ``A``, calling recursively if necessary.

    We assume that ``a`` is a nested type made of dicts, lists and scalars.

    The main usecase is to be able to treat dataclasses as a schema for json.
    Ideally, ``ofdict`` should be defined such that ``ofdict(type(x), json.loads(MyJsonEncoder().dumps(x)))`` is deep-equal to ``x`` for all ``x``.

    Similar to ` cattrs.structure <https://cattrs.readthedocs.io/en/latest/structuring.html#what-you-can-structure-and-how/>`_.
    """
    if A is Any:
        return a  # type: ignore
    if A is type(None) and a is None:
        return a  # type: ignore
    if get_origin(A) is Literal:
        values = get_args(A)
        if a in values:
            return a  # type: ignore
        else:
            logger.warning(f"Expected one of {values}, got {a}")
    if get_origin(A) is Union:
        es = []
        for X in get_args(A):
            try:
                return ofdict(X, a)
            except Exception as e:
                es.append(e)
        # [todo] raise everything?
        raise es[-1]
    od = getattr(A, "__ofdict__", None)
    if od is not None:
        return od(a)
    adapt = getattr(A, "__adapt__", None)
    if adapt is not None:
        result = adapt(a)
        if result is not None:
            return result
    if is_dataclass(A):
        d2 = {}
        for f in fields(A):
            if not isinstance(a, dict):
                raise TypeError(
                    f"Error while decoding dataclass {A}, expected a dict but got {a} : {type(a)}"
                )
            k = f.name
            if k not in a:
                if f.type is not None and is_optional(f.type):
                    v = None
                else:
                    raise ValueError(
                        f"Missing {f.name} on input dict. Decoding {a} to type {A}."
                    )
            else:
                v = a[k]
            if f.type is not None:
                d2[k] = ofdict(f.type, v)
            else:
                d2[k] = v
        return A(**d2)
    if A in [float, str, int, bytes]:  # [todo] etc
        if isinstance(a, A):
            return a
        else:
            raise TypeError(f"Expected an {A} but was {type(a)}")

    if (not get_origin(A)) and isinstance(a, A):
        return a
    raise NotImplementedError(f"No implementation of ofdict for {A}.")


@ofdict.register(list)
def _list_ofdict(A, a):
    if not isinstance(a, list):
        raise TypeError(f"Expected a list but got a {type(a)}")
    X = as_list(A)
    if X is not None:
        return [ofdict(X, y) for y in a]
    else:
        return a


@ofdict.register(dict)
def _dict_ofdict(A, a):
    if not isinstance(a, dict):
        raise TypeError(f"Expected a {A} but got {type(a)}")
    o = get_origin(A)
    if o is None:
        return a
    K, V = get_args(A)
    return o({ofdict(K, k): ofdict(V, v) for k, v in a.items()})


@ofdict.register(Enum)
def _ofdict_enum(A, a):
    return A(a)


@ofdict.register(datetime)
def _ofdict_datetime(_, t):
    return datetime.fromisoformat(t)


@ofdict.register(Path)
def _ofdict_path(_, t):
    return Path(t)


class TypedJsonDecoder(json.JSONDecoder):
    """Given a python type T, this will decode a json object to an instance of `T`, or fail otherwise.

    It makes use of the `ofdict` function defined above to convert plain json dictionaries to native python types.
    """

    def __init__(self, T: Type):
        self.T = T
        super().__init__()

    def decode(self, j):
        jj = super().decode(j)
        return ofdict(self.T, jj)


@classdispatch
def validate(t: Type, item) -> bool:
    """Validates that the given item is of the given type."""
    # [todo] type assertion `bool ??? item is t`
    if t == Any:
        return True
    o = as_optional(t)
    if o is not None:
        if t is None:
            return True
        else:
            return validate(o, item)
    X = as_list(t)
    if X is not None:
        assert isinstance(item, list)

        return all([validate(X, x) for x in item])

    if isinstance(item, t):
        if is_dataclass(item):
            return all(
                [
                    validate(field.type, getattr(item, field.name))
                    for field in fields(item)
                ]
            )
        return True
    raise NotImplementedError(f"Don't know how to validate {t}")


@singledispatch
def todict(x: Any) -> JsonLike:
    """Converts the given object to a JSON-compatible object.

    This should not recurse on the arguments. Just return one of dict, list, tuple, str, int, float, bool, or None.
    `MyJsonEncoder` will run todict recursively on any child objects.
    """
    if isinstance(x, (str, int, float, bool)):
        return x
    if x is None:
        return x
    # Something like PEP-246
    conform = getattr(x, "__todict__", None)
    if conform is not None:
        return conform()
    conform = getattr(x, "__conform__", None)
    if conform is not None:
        result = conform(JsonLike)
        # note in PEP-246 failing to conform will return None,
        # but None is a valid JsonLike so instead we use NotImplemented.
        if result is not NotImplemented:
            return result
    if isinstance(x, tuple):
        return list(x)
    if isinstance(x, (list, dict)):
        return x
    if is_dataclass(x):
        r = {}
        for field in fields(x):
            k = field.name
            v = getattr(x, k)
            if is_optional(field.type) and v is None:
                continue
            r[k] = todict(v)
        return r
    raise NotImplementedError(f"Don't know how to convert {type(x)}")


@todict.register(Enum)
def _todict_enum(x):
    return x.value


@todict.register(datetime)
def _todict_datetime(x):
    return x.isoformat()


@todict.register(Path)
def _todict_path(x):
    return str(x)


class MyJsonEncoder(json.JSONEncoder):
    """Converts Python objects to Json.

    We have additional support for dataclasses and enums that are not present in the standard encoder.
    """

    def encode(self, obj):
        if isinstance(obj, dict):
            # json encoder doesn't recursively encode keys.
            obj = {todict_key(k): v for k, v in obj.items()}
            assert all(is_json_key(k) for k in obj.keys())
        return super().encode(obj)

    # [todo] needs to handle `None` by not setting json field.
    def default(self, o):
        j = todict(o)
        if j is NotImplemented:
            j = json.JSONEncoder.default(self, o)

        return j


@singledispatch
def todict_key(x: Any) -> JsonKey:
    """Converts the given object to a JSON-compatible dictionary key (ie a string or number)."""
    if is_json_key(x):
        return x
    tdk = getattr(x, "__todict_key__", None)
    if tdk is not None:
        r = tdk()
        if not is_json_key(r):
            raise TypeError(
                f"{type(x)}.__todict_key__ returned {type(r)} instead of str or int."
            )
        return r
    j = todict(x)
    if j is NotImplemented:
        raise NotImplementedError(
            f"Don't know how to convert {type(x)} to a dictionary key."
        )
    elif not is_json_key(j):
        raise TypeError(
            f"Type {type(x)} is being used as a dictionary key but {type(j)} is not a string or integer. "
        )
    else:
        return j


def todict_rec(x: Any) -> JsonLike:
    """Recursive version of `todict`."""
    if x is NotImplemented:
        raise TypeError(f"Oops")
    assert x is not NotImplemented
    if x is None:
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    j = todict(x)
    if j is NotImplemented:
        if isinstance(x, (dict, list, tuple, str, int, float, bool)):
            j = x
        else:
            raise NotImplementedError(f"Don't know how to todict {type(x)}")
    if isinstance(j, dict):
        return {todict_key(k): todict_rec(v) for k, v in j.items()}
    elif isinstance(j, (list, tuple)):
        return [todict_rec(c) for c in j]
    elif isinstance(x, (str, int, float, bool)):
        return x
    else:
        raise TypeError(f"Don't know how to todict {type(x)}")


try:
    from pydantic import BaseModel

    @todict.register(BaseModel)
    def _todict_model(x: BaseModel):
        return x.dict()

    @ofdict.register(BaseModel)
    def _ofdict_model(ModelCls: type[BaseModel], item):
        return ModelCls.parse_obj(item)

    # [todo] pydantic validation types like EmailStr etc
except ImportError:
    pass
