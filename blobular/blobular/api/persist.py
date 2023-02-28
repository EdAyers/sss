from typing import Optional
from uuid import uuid4, UUID
from dataclasses import dataclass, field
import logging

import psycopg

from dxd import Schema, col, Table, engine, Engine
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


@dataclass
class BlobularApiDatabase:
    engine: Engine
    users: Table[User]
    api_keys: Table[ApiKey]
    blobs: Table[BlobClaim]


async def get_db():
    """FastAPI fixture for the database"""
    cfg = Settings.current()
    with psycopg.connect(cfg.pg) as conn:
        logger.info(f"Connected to {conn}")
        with engine(PsycopgEngine(conn)) as eng:
            users = User.create_table()
            aks = ApiKey.create_table(references={ApiKey.user_id: users})
            blobs = BlobClaim.create_table(references={BlobClaim.user_id: users})
            db = BlobularApiDatabase(users=users, api_keys=aks, blobs=blobs, engine=eng)
            yield db
