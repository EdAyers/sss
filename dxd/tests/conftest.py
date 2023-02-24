import pytest
from sqlite3 import Connection
import sqlite3
from dxd import engine

from dxd.sqlite_engine import SqliteEngine


@pytest.fixture(scope="module")
def sqlite_engine():
    with sqlite3.connect(":memory:") as con:
        with engine(SqliteEngine(con)) as eng:
            yield eng


@pytest.fixture(scope="module")
def postgres_engine():
    try:
        import psycopg
        from dxd.postgres_engine import PsycopgEngine
    except ImportError:
        pytest.skip("psycopg not installed")

    with psycopg.connect(
        host="localhost", port=5432, dbname="test", user="edward", password=""
    ) as conn:
        with engine(PsycopgEngine(conn)) as eng:
            yield eng


@pytest.fixture(scope="module", params=["sqlite_engine", "postgres_engine"])
def db_engine(request: pytest.FixtureRequest):
    yield request.getfixturevalue("sqlite_engine")
