import tempfile
from typing import IO, Optional
import logging
from miniscutil import human_size

from blobular.util import chunked_read
from .abstract import AbstractBlobStore, BlobInfo, get_digest_and_length
# [todo] shouldn't really depend on cli
from ..cli.cloudutils import request
from ..cli.console import tape_progress
import requests

logger = logging.getLogger(__name__)


class CloudBlobStore(AbstractBlobStore):
    """Methods for getting blobs from the cloud."""

    def __init__(self):
        pass

    def has(self, digest: str) -> bool:
        """Returns true if the blob exists on the cloud.

        If disconnected raises a ConnectionError.
        """
        r = request("HEAD", f"/blob/{digest}")
        if r.status_code == 404:
            return False
        if r.status_code // 100 == 2:
            return True
        r.raise_for_status()
        raise NotImplementedError(f"Unhandled status {r.status_code}: {r.text}")

    def get_content_length(self, digest: str) -> int:
        r = request("HEAD", f"/blob/{digest}")
        if r.status_code == 404:
            raise FileNotFoundError(f"Blob {digest} not found")
        # HEAD should never return a body
        # https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods/HEAD
        assert r.status_code == 204
        if "Content-Length" not in r.headers:
            raise RuntimeError(f"No Content-Length header for blob {digest}")
        else:
            return int(r.headers["Content-Length"])

    def add(
        self,
        tape: IO[bytes],
        digest: Optional[str] = None,
        content_length: Optional[int] = None,
        label=None,
    ) -> BlobInfo:
        """Upload the blob to the cloud.

        If the blob is already present on the cloud, the blob info is returned.
        If digest and content_length are given, they are trusted.

        Raises:
            ConnectionError: We are not connected to the cloud.
        """
        if digest is None or content_length is None:
            tape.seek(0)
            digest, content_length = get_digest_and_length(tape)
        if self.has(digest):
            logger.debug(f"Blob is already uploaded. {digest}")
            return BlobInfo(digest, content_length)
        tape.seek(0)
        pp_label = label or "unlabelled file"
        with tape_progress(
            tape,
            content_length,
            message=f"Uploading {pp_label} ({human_size(content_length)}) {digest}.",
            description="Uploading",
        ) as tape:
            r = request("PUT", "/blob", data=chunked_read(tape))
        r.raise_for_status()
        if label is not None:
            logger.debug(f"Uploaded {pp_label} {digest}.")
        return BlobInfo(digest, content_length)

    def open(self, digest: str) -> IO[bytes]:
        """Downloads the given blob to a temporary file.

        This will always cause a download.

        Raises:
            FileNotFoundError: The blob does not exist on the cloud.
            ConectionError: We are not connected to the cloud.
        """
        if not self.has(digest):
            raise FileNotFoundError(f"No blob found {digest}")
        logger.debug(f"Downloading file {digest}.")
        r = request("GET", f"/blob/{digest}")
        content_length = r.headers.get("Content-Length", None)
        if content_length is not None:
            content_length = int(content_length)
        tape = tempfile.SpooledTemporaryFile()
        with tape_progress(
            tape,
            total=content_length,
            message=f"Downloading {digest}",
            description="Downloading",
        ) as tape:
            for chunk in r.iter_content(chunk_size=2**20):
                tape.write(chunk)
        tape.seek(0)
        return tape

    def delete(self, digest: str) -> None:
        request("DELETE", f"/blob/{digest}")