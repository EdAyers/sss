from enum import Enum
import logging
from sqlite3 import PrepareProtocol as P, Connection
import datetime
import textwrap
from typing import Any, NewType, Type
import uuid
from miniscutil import as_optional, register_adapter, as_newtype
from .engine import Engine

logger = logging.getLogger("dxd.sqlite3")


def ident(x):
    return x


register_adapter(str, P)(ident)
register_adapter(int, P)(ident)
register_adapter(float, P)(ident)
register_adapter(bool, P)(ident)
register_adapter(bytes, P)(ident)
register_adapter(type(None), P)(ident)


@register_adapter(Enum, P)
def adapt_enum(obj: Enum):
    assert issubclass(type(obj), Enum)
    assert type(obj) is not Enum
    return obj.value


# [todo] use pydantic
register_adapter(datetime.datetime, P)(lambda o: o.isoformat())
register_adapter(uuid.UUID, P)(lambda u: u.bytes)


class SqliteEngine(Engine):
    mode = "sqlite"

    def __init__(self, connection: Connection):
        self.connection = connection

    @property
    def protocol(self):
        return P

    def execute(self, query, values=()):
        msg = textwrap.indent(str(query) + "\n" + str(values), " " * 4)
        logger.debug(f"SqliteEngine.execute:\n{msg}")
        return self.connection.execute(query, values)

    def executemany(self, query: str, values):
        msg = textwrap.indent(str(query), " " * 4)
        logger.debug(f"SqliteEngine.executemany {len(values)}:\n{msg}")
        return self.connection.executemany(query, values)

    def transaction(self):
        with self.connection:
            yield self

    def commit(self):
        self.connection.commit()

    def get_storage_type(self, T: Type):
        def core(T: Type):
            S = as_newtype(T)
            if S is not None:
                return core(S)
            if not isinstance(T, type):
                raise TypeError(f"{T} is not a type")
            if issubclass(T, str):
                return "TEXT"
            elif issubclass(T, int):
                return "INTEGER"
            elif issubclass(T, float):
                return "REAL"
            elif issubclass(T, bool):
                return "INTEGER"
            elif issubclass(T, bytes):
                return "BLOB"
            elif issubclass(T, datetime.datetime):
                return "timestamp"
            elif issubclass(T, uuid.UUID):
                return "BLOB"
            elif issubclass(T, Enum):
                V = list(T)[0]
                return core(type(V.value))
            else:
                return ""
                # raise ValueError(f"No storage type registered for {T}")

        X = as_optional(T)
        if X is None:
            return core(T) + " NOT NULL"
        else:
            return core(X)
