from decimal import Decimal
from enum import Enum
import textwrap
from uuid import UUID
import psycopg
import logging
import datetime

from miniscutil import as_optional

from .engine import Engine

logger = logging.getLogger("dxd.postgres")


class PostgresEncodable:
    pass


type_map = {
    bool: "boolean",
    int: "integer",
    float: "float8",
    Decimal: "numeric",
    str: "text",
    bytes: "BYTEA",
    datetime.date: "date",
    datetime.datetime: "timestamp",
    datetime.time: "time",
    datetime.timedelta: "interval",
    UUID: "uuid",
}


class PsycopgEngine(Engine):
    mode = "postgresql"

    connection: psycopg.Connection

    def __init__(self, connection: psycopg.Connection):
        self.connection = connection

    @property
    def protocol(self):
        return PostgresEncodable

    def adapt(self, obj):
        if type(obj) in type_map:
            return obj
        elif isinstance(obj, Enum):
            return obj.value
        elif obj is None:
            return None
        else:
            raise NotImplementedError(f"Cannot adapt {type(obj)}")

    def restore(self, T, obj):
        if isinstance(obj, T):
            return obj
        return super().restore(T, obj)

    def execute(self, query, params=()):
        query = query.replace("?", "%s")
        msg = textwrap.indent(str(query) + "\n" + str(params), " " * 4)
        logger.debug(f"PsycopgEngine.execute:\n{msg}")
        return self.connection.execute(query, params)

    def executemany(self, query, params=[]):
        query = query.replace("?", "%s")
        msg = textwrap.indent(str(query), " " * 4)
        logger.debug(f"PsycopgEngine.execute:\n{msg}")
        cur = self.connection.cursor()
        cur.executemany(query, params)
        return cur

    def get_storage_type(self, T):
        def core(T):
            if T in type_map:
                return type_map[T]
            elif issubclass(T, Enum):
                V = type(list(T)[0].value)
                return core(V)
            else:
                raise TypeError(f"Can't convert {T} to storage type")

        X = as_optional(T)
        if X is not None:
            return core(X)
        else:
            return core(T) + " NOT NULL"

    def transaction(self):
        with self.connection.transaction():
            yield self
