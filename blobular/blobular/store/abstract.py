from dataclasses import dataclass
from typing import IO, Iterable, Optional, Tuple

from blake3 import blake3

from miniscutil import chunked_read


@dataclass
class BlobInfo:
    digest: str
    content_length: int


def get_digest_and_length(tape: IO[bytes]) -> tuple[str, int]:
    content_length = 0
    h = blake3()
    for data in chunked_read(tape):
        content_length += len(data)
        h.update(data)
    digest = h.hexdigest()
    return (digest, content_length)


class AbstractBlobStore:
    @property
    def id(self) -> str:
        """Unique resource identifier for the blob store."""
        raise NotImplementedError()

    def open(self, digest: str) -> IO[bytes]:
        """Returns a file-like object for the given blob digest.

        Consumer is responsible for closing the file-like object.
        """
        raise NotImplementedError()

    def delete(self, digest: str):
        raise NotImplementedError()

    def add(
        self,
        tape: IO[bytes] | bytes,
        *,
        digest: str | None = None,
        content_length: Optional[int] = None,
    ) -> BlobInfo:
        """Save the tape file to the blob store.

        If digest or content_length are given, they are trusted."""
        raise NotImplementedError()

    def has(self, digest: str) -> bool:
        raise NotImplementedError()

    def get_info(self, digest: str) -> Optional[BlobInfo]:
        raise NotImplementedError()

    def iter(self) -> Iterable[str]:
        raise NotImplementedError()

    def clear(self):
        raise NotImplementedError()