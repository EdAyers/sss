from enum import Enum
from datetime import datetime
from dataclasses import dataclass
import logging
import psycopg
import sqlite3
from typing import Optional

from dxd import col, Table, engine, transaction, Engine, Schema
from dxd.postgres_engine import PsycopgEngine
from dxd.sqlite_engine import SqliteEngine


class BlobStatus(Enum):
    foo = 1
    bar = 2


@dataclass
class Blobb(Schema):
    digest: str = col(primary=True)
    length: int = col()
    status: BlobStatus = col()
    label: Optional[str] = col(default=None)
    created: datetime = col(default_factory=datetime.now)
    accesses: int = col(default=0)


def test_it(db_engine: Engine):
    logging.basicConfig(level=logging.DEBUG)
    blobs = Blobb.create_table(engine=db_engine)
    blobs.clear()

    assert blobs.connection is db_engine

    blobs.insert_many(
        [
            Blobb(
                digest="cabbage",
                length=1000,
                label=None,
                status=BlobStatus.foo,
            ),
            Blobb(
                digest="beef",
                length=1001,
                label="hello",
                status=BlobStatus.foo,
            ),
        ]
    )

    blob = blobs.select_one(where=Blobb.digest == "cabbage")
    assert blob is not None
    assert isinstance(blob, Blobb)
    assert blob.digest == "cabbage"
    assert blob.label is None
    assert blob.length == 1000

    t = datetime.now()

    i = blobs.update(
        {
            Blobb.accesses: Blobb.accesses + 1,
            Blobb.created: t,
            Blobb.status: BlobStatus.bar,
        },
        where=Blobb.status == BlobStatus.foo,
    )
    assert i == 2

    blob2 = blobs.select_one(where=Blobb.digest == blob.digest)
    assert blob2 is not None
    assert blob2.status == BlobStatus.bar
    assert blob2.accesses == blob.accesses + 1
    assert blob2.created == t

    for digest, label in blobs.select(
        where=(Blobb.status == BlobStatus.bar), select=(Blobb.digest, Blobb.label)
    ):
        print(digest, label)

    # Blobb.drop()


if __name__ == "__main__":
    with psycopg.connect(
        host="localhost", port=5432, dbname="test", user="edward", password=""
    ) as conn:
        with engine(PsycopgEngine(conn)) as eng:
            test_it(eng)

    # with sqlite3.connect(":memory:") as conn:
    #     with engine(SqliteEngine(conn)) as eng:
    #         test_it(eng)
