from io import BytesIO
from typing import IO, Any, Optional
from blobular.store.abstract import BlobInfo, get_digest_and_length, AbstractBlobStore

"""
Example client setup:

cfg = Settings.current()

client = boto3.client('s3',
    aws_access_key_id=cfg.aws_access_key_id,
    aws_secret_access_key=cfg.aws_secret_access_key,
)

"""


class S3BlobStore(AbstractBlobStore):
    client: Any

    def __init__(self, bucket_name, client):
        self.client = client
        self.spill_size = 2**20
        self.bucket_name = bucket_name

    def open(self, digest: str):
        # [todo] consider offering a redirect to a temporary S3 URL
        obj = self.client.get_object(Bucket=self.bucket_name, Key=digest)
        return obj["Body"]

    def add(
        self,
        tape: IO[bytes] | bytes,
        *,
        digest: Optional[str] = None,
        content_length: Optional[int] = None,
    ):
        if isinstance(tape, bytes):
            tape = BytesIO(tape)

        if digest is None or content_length is None:
            digest, content_length = get_digest_and_length(tape)
            tape.seek(0)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=digest,
            Body=tape,
            ContentLength=content_length,
            Metadata={
                "digest": digest,
                "content_length": str(content_length),
            },
        )

        return BlobInfo(digest=digest, content_length=content_length)

    def delete(self, digest: str):
        self.client.delete_object(Bucket=self.bucket_name, Key=digest)

    def has(self, digest: str) -> bool:
        return self.get_info(digest) is not None

    def get_info(self, digest: str) -> Optional[BlobInfo]:
        head = self.client.head_object(Bucket=self.bucket_name, Key=digest)
        assert head["Metadata"]["digest"] == digest
        return BlobInfo(
            digest=digest,
            content_length=int(head["ContentLength"]),
        )

    def iter(self):
        raise NotImplementedError("refusing to iterate over S3 blobs")
