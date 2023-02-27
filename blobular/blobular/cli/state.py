import sqlite3
from dataclasses import dataclass, fields, asdict
from enum import Enum
from .settings import Settings, APP_NAME

from ..__about__ import __version__ as version
from .console import is_interactive_terminal

from ..store import (
    AbstractBlobStore,
    SizedBlobStore,
    CacheBlobStore,
    InMemBlobStore,
    OnDatabaseBlobStore,
    LocalFileBlobStore,
    CacheRow,
    BlobContent,
)
from ..store.cloud import CloudBlobStore

from miniscutil import Current
from dxd import Table, engine_context
from dxd.sqlite_engine import SqliteEngine
from pathlib import Path


@dataclass
class AppState(Current):
    local_file_store: LocalFileBlobStore
    cloud_store: CloudBlobStore
    store: CacheBlobStore

    @classmethod
    def of_dir(cls, dir: Path):
        db_path = dir / "local.db"
        blobspath = dir / "blobs"
        blobspath.mkdir(exist_ok=True, parents=True)
        conn = sqlite3.connect(db_path)
        engine = SqliteEngine(conn)
        engine_context.set(engine)
        cache_table = CacheRow.create_table("blobs", engine)
        result_table = BlobContent.create_table("results", engine)
        local_file_store = LocalFileBlobStore(blobspath)
        cloud_store = CloudBlobStore()
        blobstore = CacheBlobStore(
            SizedBlobStore(OnDatabaseBlobStore(result_table), local_file_store),
            cloud_store,
            table=cache_table,
        )
        return cls(
            store=blobstore, local_file_store=local_file_store, cloud_store=cloud_store
        )

    @classmethod
    def default(cls):
        cfg = Settings.current()
        return cls.of_dir(cfg.local_cache_dir)
