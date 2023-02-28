from pathlib import Path
from stat import S_IREAD, S_IRGRP
from typing import IO, Optional

import logging

from ..util import chunked_read

from .abstract import BlobInfo, get_digest_and_length, AbstractBlobStore

logger = logging.getLogger("blobular")


class LocalFileBlobStore(AbstractBlobStore):
    """Everything to do with storing blobs locally on disk.

    We don't do anything with connecting to the local blobs tracking db or storing blobs
    directly on the local database.
    """

    def __init__(self, local_cache_dir: Path):
        self.local_cache_dir = local_cache_dir

    def iter(self):
        """Iterate all of the digests of the blobs that exist on disk."""
        p = self.local_cache_dir
        for bp in p.iterdir():
            if bp.is_file():
                digest = bp.name
                yield digest

    def local_file_cache_path(self, digest: str):
        """Gets the place where the blob would be stored. Note that this doesn't guarantee existence."""
        p = self.local_cache_dir
        return p / digest

    def has(self, digest: str) -> bool:
        """Checks whether the blob exists __locally__."""
        return self.local_file_cache_path(digest).exists()

    def get_info(self, digest) -> Optional[BlobInfo]:
        p = self.local_file_cache_path(digest)
        if not p.exists():
            return None
        content_length = p.stat().st_size
        return BlobInfo(digest, content_length)

    def delete(self, digest):
        """Deletes the given blob from the local cache.

        Note that some directories etc may symlink to this blob, so you should do this with care.
        """
        p = self.local_file_cache_path(digest)
        if p.exists():
            p.unlink()
            logger.debug(f"Deleted local blob {digest}")

    def clear(self):
        for d in list(self.iter()):
            self.delete(d)

    def open(self, digest: str, **kwargs) -> IO:
        """Opens the blob. You are responsible for closing it.

        Will throw FileNotFoundError if the blob doesn't exist.
        """
        if not self.has(digest):
            raise FileNotFoundError(f"No blob {digest}")
        return open(self.local_file_cache_path(digest), mode="rb", **kwargs)

    def add(
        self,
        tape: IO[bytes],
        *,
        digest: Optional[str] = None,
        content_length: Optional[int] = None,
        label: Optional[str] = None,
    ) -> BlobInfo:
        """Saves a blob to the local store.

        If digest and content_length is given, it is trusted.
        """
        if digest is None or content_length is None:
            tape.seek(0)
            digest, content_length = get_digest_and_length(tape)
        tape.seek(0)

        if not self.has(digest):
            cp = self.local_file_cache_path(digest)
            # [todo] smaller blobs (< 2**20) should be stored in a sqlite table or
            # other kv store system.
            # [todo] exclusive file lock.
            with open(cp, "wb") as c:
                for data in chunked_read(tape):
                    c.write(data)
            # blobs are read only.
            # ref: https://stackoverflow.com/a/28492823/352201
            cp.chmod(S_IREAD | S_IRGRP)
            # [todo] what about S_IROTH?
        return BlobInfo(digest, content_length)
