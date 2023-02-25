from typing import Optional
from uuid import uuid4, UUID
from datetime import datetime
from dataclasses import dataclass, field
from dxd import Schema, col


@dataclass
class BlobClaim(Schema):
    last_accessed: datetime = col(default_factory=datetime.utcnow)
    content_length: int = col()
    accesses: int = col(default=0)
    digest: str = col(primary=True)
    user_id: Optional[UUID] = col(foreign_key=True, primary=True)

    # [todo] access control goes here; eg groupid and chmod
    is_public: bool = col(default=False)