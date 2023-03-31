""" Simple sqlite persistence layer for uxu """
from pathlib import Path
import sqlite3
from typing import Generic, Iterable, Type, TypeVar
from miniscutil.ofdict import TypedJsonDecoder, MyJsonEncoder
import json

T = TypeVar("T")


class PersistDict(Generic[T]):
    """Super simple sqlite backed dictionary with json serialisation."""

    def __init__(self, path: Path, T: Type[T], table_name: str = "dict"):
        self.table_name = table_name
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        self.T = T
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name} (key TEXT PRIMARY KEY, value BLOB);"
        )

    def get(self, key: str) -> T:
        cur = self.conn.execute(
            f"SELECT value FROM {self.table_name} WHERE key=?; ", (key,)
        )
        item = cur.fetchone()
        if item is None:
            raise KeyError(key)
        r = json.loads(item[0], cls=TypedJsonDecoder, T=self.T)
        return r

    def pop(self, key) -> T:
        with self.conn:  # make a transaction
            r = self.get(key)
            self.conn.execute(f"DELETE FROM {self.table_name} WHERE key=? ; ", (key,))
            return r

    def set(self, key: str, value: T):
        v = json.dumps(value, cls=MyJsonEncoder)
        self.conn.execute(
            f"INSERT OR REPLACE INTO {self.table_name} (key, value) VALUES (?,?) ; ",
            (key, v),
        )

    def clear(self):
        self.conn.execute(f"DELETE FROM {self.table_name};")

    def items(self) -> Iterable[tuple[str, T]]:
        with self.conn:  # make a transaction
            cur = self.conn.execute(f"SELECT key, value FROM {self.table_name};")
            items = list(cur.fetchall())
        for key, v in items:
            yield key, json.loads(v, cls=TypedJsonDecoder, T=self.T)
