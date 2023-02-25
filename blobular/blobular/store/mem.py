from dataclasses import dataclass
import io
from typing import IO
import warnings

from ..util import human_size
from .abstract import AbstractBlobStore, BlobInfo, get_digest_and_length


class InMemBlobStore(AbstractBlobStore):
    blobs: dict[str, bytes]
    max_size: int | None

    def __init__(self, max_size=2**10):
        self.max_size = max_size
        self.blobs = {}

    def open(self, digest: str) -> IO[bytes]:
        if digest not in self.blobs:
            raise FileNotFoundError(f"No blob with digest {digest}")
        return io.BytesIO(self.blobs[digest])

    def add(
        self, tape: IO[bytes] | bytes, *, digest=None, content_length=None
    ) -> BlobInfo:
        if isinstance(tape, bytes):
            tape = io.BytesIO(tape)
        assert isinstance(tape, IO)
        tape.seek(0)
        if digest is None or content_length is None:
            digest, content_length = get_digest_and_length(tape)
            tape.seek(0)
        if self.max_size is not None and content_length > self.max_size:
            raise ValueError(
                f"Adding an in-mem blob with size {human_size(content_length)} is too large (max is set to {human_size(self.max_size)})."
            )
        self.blobs[digest] = tape.read()
        return BlobInfo(digest=digest, content_length=content_length)

    def has(self, digest: str) -> bool:
        return digest in self.blobs

    def delete(self, digest: str) -> None:
        if digest in self.blobs:
            del self.blobs[digest]
