from dataclasses import MISSING
from pathlib import Path
import sqlite3
from typing import Generic, Iterable, Literal, Optional, Type, TypeVar, Union
from miniscutil.ofdict import TypedJsonDecoder, MyJsonEncoder
import json

from dxd.engine import Engine, engine_context, transaction

T = TypeVar("T")


class KVStore(Generic[T]):
    """Super simple db backed dictionary with json serialisation."""

    def __init__(
        self, T: Type[T], table_name: str = "dict", engine: Optional[Engine] = None
    ):
        self.engine = engine or engine_context.get()
        self.table_name = table_name
        self.T = T
        self.engine.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (key TEXT PRIMARY KEY, value BLOB);"
        )

    # [todo] think about abstracting encode/decode to a 'Serde' class. NIH?

    def decode(self, value: bytes) -> T:
        return json.loads(value, cls=TypedJsonDecoder, T=self.T)

    def encode(self, value: T) -> bytes:
        return json.dumps(value, cls=MyJsonEncoder).encode()

    def get(self, key: str, default: Union[T, Literal[MISSING]] = MISSING) -> T:
        cur = self.engine.execute(
            f"SELECT value FROM {self.table_name} WHERE key=? ;", (key,)
        )
        item = cur.fetchone()
        if item is None:
            if default is not MISSING:
                return default
            else:
                raise KeyError(key)
        r = self.decode(item[0])
        return r

    def pop(self, key: str) -> T:
        with transaction(self.engine):
            r = self.get(key)
            self.engine.execute(f"DELETE FROM {self.table_name} WHERE key=? ;", (key,))
            return r

    def set(self, key: str, value: T):
        v = self.encode(value)
        self.engine.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?,?);",
            (key, v),
        )

    def clear(self):
        self.engine.execute(f"DELETE FROM {self.table_name};")

    def items(self) -> Iterable[tuple[str, T]]:
        cur = self.engine.execute(f"SELECT key, value FROM {self.table_name};")
        for key, v in cur.fetchall():
            yield key, self.decode(v)

    def keys(self) -> Iterable[str]:
        for (x,) in self.engine.execute(
            f"SELECT key FROM {self.table_name};"
        ).fetchall():
            yield x

    def values(self):
        for _, v in self.items():
            yield v

    def __contains__(self, key: str) -> bool:
        return (
            self.engine.execute(
                f"SELECT value FROM {self.table_name} WHERE key=? ;", (key,)
            ).fetchone()
            is not None
        )
