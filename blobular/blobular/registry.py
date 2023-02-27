from typing import Optional
from uuid import uuid4, UUID
from datetime import datetime
from dataclasses import dataclass, field
from dxd import Schema, col


@dataclass
class BlobClaim(Schema):
    content_length: int = col()
    digest: str = col(primary=True)
    user_id: Optional[UUID] = col(foreign_key=True, primary=True)

    accesses: int = col(default=0)
    is_public: bool = col(default=False)
    last_accessed: datetime = col(default_factory=datetime.utcnow)

    created: datetime = col(default_factory=datetime.utcnow)
    label: Optional[str] = col(default=None)
    content_type: Optional[str] = col(default=None)

    # [todo] access control goes here; eg groupid and chmod
