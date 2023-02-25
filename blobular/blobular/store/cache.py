from dataclasses import dataclass
from datetime import datetime
from typing import IO, Literal
from dxd import col, Schema, Table, sum

from blobular.store.abstract import AbstractBlobStore, BlobInfo, get_digest_and_length

""" Caching blobstore """


@dataclass
class CacheRow(Schema):
    accesses: int = col(default=0)
    last_accessed: datetime = col(default_factory=datetime.utcnow)
    digest: str = col(primary=True)
    content_length: int = col()
    is_cached: bool = col(default=False)
    is_stored: bool = col(default=False)


class CacheBlobStore:
    size: int

    def __init__(
        self,
        cache: AbstractBlobStore,
        store: AbstractBlobStore,
        table: Table[CacheRow],  # [note] this should be in sqlite not in postgres
        policy: Literal["lru", "least_accessed"] = "lru",
        max_size: int = 2**30,
    ):
        self.cache = cache
        self.store = store
        self.table = table
        self.policy = policy
        self.max_size = max_size
        self.recalc_cache_size()

    def recalc_cache_size(self) -> int:
        size = self.table.select_one(
            where=CacheRow.is_cached == True, select=sum(CacheRow.content_length)
        )
        assert isinstance(size, int)
        self.size = size
        return size

    def touch(self, digest):
        self.table.update(
            where=CacheRow.digest == digest,
            values={
                CacheRow.accesses: CacheRow.accesses + 1,
                CacheRow.last_accessed: datetime.utcnow(),
            },
        )

    def open(self, digest):
        self.touch(digest)
        if self.cache.has(digest):
            return self.cache.open(digest)
        else:
            self.pull(digest)
            return self.store.open(digest)

    def get_info(self, digest):
        row = self.table.select_one(where=CacheRow.digest == digest)
        if row is None:
            return self.store.get_info(digest)
        return BlobInfo(digest=row.digest, content_length=row.content_length)

    def has(self, digest):
        return self.get_info(digest) is not None

    def evict(self, space_needed: int):
        start_size = self.size
        n = space_needed
        if self.policy == "lru":
            # [todo] performance leaves much to be desired
            digests = []
            for L in [2**20, 0]:
                # heuristic: evict the big blobs first
                for row in self.table.select(
                    where=CacheRow.is_cached == True
                    and CacheRow.is_stored == True
                    and CacheRow.content_length > L,
                    order_by=CacheRow.last_accessed,
                    descending=False,
                ):
                    if n <= 0:
                        break
                    n -= row.content_length
                    digests.append(row.digest)
            for digest in digests:
                self.cache.delete(digest)
                self.table.update(
                    where=CacheRow.digest == digest, values={CacheRow.is_cached: False}
                )
            self.recalc_cache_size()
            assert self.size == start_size - space_needed + n
        else:
            raise NotImplementedError()

    def _add_to_cache(self, tape, *, digest, content_length):
        if content_length > self.max_size:
            raise ValueError(
                f"content_length {content_length} is too large for the cache"
            )
        if not self.cache.has(digest):
            space_needed = content_length + self.size - self.max_size
            if space_needed >= 0:
                self.evict(space_needed)
            self.cache.add(tape, digest=digest, content_length=content_length)
            self.size += content_length
        self.table.update(
            values={CacheRow.is_cached: True}, where=CacheRow.digest == digest
        )

    def _add_to_store(self, tape, *, digest, content_length):
        self.store.add(tape, digest=digest, content_length=content_length)
        self.table.update(
            values={CacheRow.is_stored: True}, where=CacheRow.digest == digest
        )

    def add(self, tape, *, digest=None, content_length=None):
        if digest is None or content_length is None:
            digest, content_length = get_digest_and_length(tape)
            tape.seek(0)

        row = self.table.select_one(where=CacheRow.digest == digest)
        if row is None:
            row = CacheRow(
                digest=digest,
                content_length=content_length,
                is_cached=False,
                is_stored=False,
            )
            self.table.insert_one(row)
        if content_length > self.max_size:
            self._add_to_store(tape, digest=digest, content_length=content_length)
        elif not row.is_cached:
            self._add_to_cache(tape, digest=digest, content_length=content_length)
        elif not row.is_stored:
            # [todo] queue up to store on a background thread (or flush)
            pass
        return BlobInfo(digest=digest, content_length=content_length)

    def pull(self, digest):
        if self.cache.has(digest):
            return
        info = self.store.get_info(digest)
        if info is None:
            raise LookupError(f"no blob in store with digest {digest}")
        with self.store.open(digest) as tape:
            self._add_to_cache(
                tape, digest=info.digest, content_length=info.content_length
            )

    def push(self, digest):
        row = self.table.select_one(
            where=CacheRow.digest == digest,
        )
        assert row is not None
        if row.is_stored:
            return
        assert row.is_cached
        with self.cache.open(row.digest) as f:
            self.store.add(f, digest=row.digest, content_length=row.content_length)
        self.table.update(where=CacheRow.digest == digest, values={CacheRow.is_stored: True})

    def flush(self):
        digests = list(self.table.select(
            where=CacheRow.is_cached == True and CacheRow.is_stored == False
        ))
        for digest in digests:
            self.push(digest)


class SizedBlobStore(AbstractBlobStore):
    """ Store where it puts blobs in small if it's less than threshold or big otherwise. """
    threshold: int

    def __init__(
        self, small: AbstractBlobStore, big: AbstractBlobStore, threshold: int = 2**20
    ):
        self.small = small
        self.big = big


    def add(self, tape : IO[bytes], *, digest=None, content_length=None):
        if digest is None or content_length is None:
            digest, content_length = get_digest_and_length(tape)
            tape.seek(0)
        if content_length > self.threshold:
            return self.big.add(tape, digest=digest, content_length=content_length)
        else:
            return self.small.add(tape, digest=digest, content_length=content_length)

    def has(self, digest):
        return self.small.has(digest) or self.big.has(digest)

    def get_info(self, digest):
        return self.small.get_info(digest) or self.big.get_info(digest)

    def open(self, digest):
        if self.small.has(digest):
            return self.small.open(digest)
        elif self.big.has(digest):
            return self.big.open(digest)
        else:
            raise LookupError(f"no blob in store with digest {digest}")