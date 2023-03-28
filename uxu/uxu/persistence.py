""" Simple sqlite persistence layer for uxu """
from pathlib import Path
import sqlite3
from typing import Generic, Iterable, Type, TypeVar
from miniscutil.ofdict import TypedJsonDecoder, MyJsonEncoder
import json

T = TypeVar("T")


class PersistDict(Generic[T]):
    """Super simple sqlite backed dictionary with json serialisation."""

    def __init__(self, path: Path, T: Type[T]):
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        self.T = T
        self.conn = sqlite3.connect(path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS dict (key TEXT PRIMARY KEY, value BLOB);"
        )

    def get(self, key: str) -> T:
        cur = self.conn.execute("SELECT value FROM dict WHERE key=?", (key,))
        item = cur.fetchone()
        if item is None:
            raise KeyError(key)
        r = json.loads(item[0], cls=TypedJsonDecoder, T=self.T)
        return r

    def pop(self, key) -> T:
        with self.conn:  # make a transaction
            r = self.get(key)
            self.conn.execute("DELETE FROM dict WHERE key=?", (key,))
            return r

    def set(self, key: str, value: T):
        v = json.dumps(value, cls=MyJsonEncoder)
        self.conn.execute(
            "INSERT OR REPLACE INTO dict (key, value) VALUES (?,?)", (key, v)
        )

    def clear(self):
        self.conn.execute("DELETE FROM dict;")

    def items(self) -> Iterable[tuple[str, T]]:
        with self.conn:  # make a transaction
            cur = self.conn.execute("SELECT key, value FROM dict;")
            items = list(cur.fetchall())
        for key, v in items:
            yield key, json.loads(v, cls=TypedJsonDecoder, T=self.T)
