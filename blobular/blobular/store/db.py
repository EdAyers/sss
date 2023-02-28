from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import io
from typing import IO, Optional
from .abstract import AbstractBlobStore, BlobInfo, get_digest_and_length
from dxd import col, Schema, Table


@dataclass
class BlobContent(Schema):
    """The value of the blob. This is stored directly in the database which is more efficient for smaller blobs."""

    content: bytes
    content_length: int
    digest: str = col(primary=True)
    accesses: int = col(default=0)
    last_accessed: datetime = col(default_factory=datetime.now)


class OnDatabaseBlobStore(AbstractBlobStore):
    def __init__(
        self,
        table: str | Table[BlobContent],
    ):
        if isinstance(table, str):
            self.table = BlobContent.create_table(name=table)
        elif isinstance(table, Table):
            self.table = table
        else:
            raise TypeError(f"Expected str or Table[BlobContent], got {type(table)}")

    def touch(self, digest: str):
        self.table.update(
            values={
                BlobContent.last_accessed: datetime.now(),
                BlobContent.accesses: BlobContent.accesses + 1,
            },
            where=BlobContent.digest == digest,
        )

    def has(self, digest: str) -> bool:
        return self.table.has(where=BlobContent.digest == digest)

    def open(self, digest: str) -> IO[bytes]:
        r = self.table.update(
            {
                BlobContent.last_accessed: datetime.now(),
                BlobContent.accesses: BlobContent.accesses + 1,
            },
            where=BlobContent.digest == digest,
            returning=BlobContent.content,
        )
        r = list(r)
        if len(r) == 0:
            raise FileNotFoundError(f"No blob with digest {digest} found")
        assert len(r) == 1, "corrupted blobstore"
        content = r[0]
        return io.BytesIO(content)

    def add(self, tape: IO[bytes] | bytes, *, digest=None, content_length=None):
        if isinstance(tape, bytes):
            tape = io.BytesIO(tape)
        tape.seek(0)
        if digest is None or content_length is None:
            digest, content_length = get_digest_and_length(tape)
            tape.seek(0)

        self.table.insert_one(
            BlobContent(
                content=tape.read(),
                content_length=content_length,
                digest=digest,
            )
        )
        return BlobInfo(digest=digest, content_length=content_length)

    def delete(self, digest: str):
        self.table.delete(where=BlobContent.digest == digest)

    def iter(self):
        yield from self.table.select(select=BlobContent.digest)

    def get_info(self, digest):
        row = self.table.select_one(where=BlobContent.digest == digest)
        if row is None:
            return None
        return BlobInfo(digest=row.digest, content_length=row.content_length)


    def clear(self):
        self.table.clear()