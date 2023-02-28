from pathlib import Path
import sqlite3
from typing import Callable, Generator, Generic, Optional, TypeVar, Union
from uuid import uuid4, UUID
from dataclasses import dataclass, field
import logging
from blobular.store import AbstractBlobStore
from blobular.store.cache import SizedBlobStore
from blobular.store.db import BlobContent, OnDatabaseBlobStore
from blobular.store.localfile import LocalFileBlobStore
from blobular.store.s3 import S3BlobStore
from dxd.sqlite_engine import SqliteEngine

import psycopg

from dxd import Schema, col, Table, engine_context, Engine
from dxd.postgres_engine import PsycopgEngine
from pydantic import PostgresDsn
from blobular.api.settings import Settings  # we are using postgres
from blobular.registry import BlobClaim

logger = logging.getLogger("blobular")


@dataclass
class User(Schema):
    gh_id: int
    gh_username: str
    gh_email: str
    gh_avatar_url: str
    email_verified: bool
    id: UUID = col(primary=True, default_factory=uuid4)
    quota: Optional[int] = col(default=2**30)


@dataclass
class ApiKey(Schema):
    key: str = col(primary=True)
    user_id: UUID = col(foreign_key=User.id)
    label: Optional[str] = col(default=None)


class BlobularApiDatabase:
    connection: Union[psycopg.Connection, sqlite3.Connection]
    engine: Engine
    users: Table[User]
    api_keys: Table[ApiKey]
    blobs: Table[BlobClaim]
    blobstore: AbstractBlobStore
    subscriptions: list[Callable[[], None]]

    def __init__(self):
        self.subscriptions = []

    @property
    def is_connected(self):
        return hasattr(self, "connection")

    def connect(self):
        cfg = Settings.current()
        if cfg.database_mode == "postgres":
            if cfg.pg is None:
                raise ValueError("no postgres url found")
            self.connection = psycopg.connect(cfg.pg)
            logger.info(f"Connected to {self.connection}")
            self.engine = PsycopgEngine(self.connection)
        else:
            cfg.local_data_path.mkdir(parents=True, exist_ok=True)
            db = cfg.local_data_path / "blobular_api.db"
            self.connection = sqlite3.connect(db, check_same_thread=False)
            self.engine = SqliteEngine(self.connection)
        engine_context.set(self.engine)
        self.subscriptions.append(self.connection.close)
        self.users = User.create_table(engine=self.engine)
        self.api_keys = ApiKey.create_table(
            references={ApiKey.user_id: self.users}, engine=self.engine
        )
        self.blobs = BlobClaim.create_table(
            references={BlobClaim.user_id: self.users}, engine=self.engine
        )

        # create the blobstore
        table = BlobContent.create_table("apiresults", self.engine)
        small = OnDatabaseBlobStore(table)

        if cfg.blobstore_mode == "s3":
            import boto3

            client = boto3.client(
                "s3",
                aws_access_key_id=cfg.aws_access_key_id,
                aws_secret_access_key=cfg.aws_secret_access_key.get_secret_value(),
            )
            big = S3BlobStore(bucket_name="blobular", client=client)
            self.subscriptions.append(client.close)
        else:
            dir = cfg.local_data_path / "blobs"
            dir.mkdir(exist_ok=True)
            big = LocalFileBlobStore(dir)
        self.blobstore = SizedBlobStore(small, big, threshold=0)  # [todo]

    def disconnect(self):
        for s in self.subscriptions:
            s()

    async def __call__(self):
        # reference: https://fastapi.tiangolo.com/advanced/advanced-dependencies/
        t = engine_context.set(self.engine)
        yield self
        engine_context.reset(t)
