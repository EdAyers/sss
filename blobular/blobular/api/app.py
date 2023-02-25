from datetime import datetime
from fastapi import FastAPI
from secrets import token_urlsafe
from typing import Optional
import logging
from uuid import UUID
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, SecretStr
from starlette.requests import Request

from blobular.registry import BlobClaim
from .authentication import (
    AuthenticationError,
    JwtClaims,
    from_request,
    user_of_token,
)
from .github_login import login_handler

from .persist import BlobularApiDatabase as Db, User, get_db, ApiKey as ApiKeyEntry
from blobular.store.abstract import AbstractBlobStore, get_digest_and_length
from miniscutil import chunked_read
from .authentication import get_user

from dxd import transaction

app = FastAPI()
logger = logging.getLogger("blobular")


def get_blobstore(request: Request) -> AbstractBlobStore:
    raise NotImplementedError()


@app.middleware("http")
async def error_handling(request: Request, call_next):
    try:
        return await call_next(request)
    except AuthenticationError as exc:
        raise HTTPException(status_code=401, detail=str(exc))
    except HTTPException as exc:
        raise exc
    except Exception as exc:
        logger.exception("internal server error")
        raise HTTPException(status_code=500, detail="internal server error")


@app.get("/user/")
async def handle_get_user(user: User = Depends(get_user)):
    """Get the current user."""
    return {
        "gh_id": user.gh_id,
        "gh_avatar_url": user.gh_avatar_url,
        "gh_username": user.gh_username,
        "id": user.id,
    }


@app.post("/user/login")
async def login(code: str, state: str | None = None):
    """Login to the server."""
    return await login_handler(code)


@app.post("/api_key/generate")
def generate_api_key(request: Request, db: Db = Depends(get_db)):
    token = from_request(request)
    if not isinstance(token, JwtClaims):
        raise AuthenticationError(
            "you must be authenticated with a JWT to create an API key"
        )
    user = user_of_token(token, db)
    key = "hs-" + token_urlsafe(16)
    db.api_keys.insert_one(ApiKeyEntry(key=key, user_id=user.id))
    return key


@app.put("/blob")
async def put_blob(
    file: UploadFile,  # [todo] make optional, so you can change settings on a blob without uploading.
    is_public: bool = False,
    blobstore: AbstractBlobStore = Depends(get_blobstore),
    user=Depends(get_user),
    db: Db = Depends(get_db),
):
    """Upload a blob."""
    info = blobstore.add(file.file)
    # [todo] perform in single query
    with transaction():
        where = BlobClaim.user_id == user.id and BlobClaim.digest == info.digest
        b = db.blobs.select_one(where=where)
        assert b is None or b.content_length == info.content_length
        if b is None:
            db.blobs.insert_one(
                BlobClaim(
                    user_id=user.id,
                    digest=info.digest,
                    content_length=info.content_length,
                    is_public=is_public,
                )
            )
        else:
            if is_public and not b.is_public:
                db.blobs.update(
                    where=where,
                    values={"is_public": True},
                )
            if b.is_public:
                is_public = True
    return {
        "digest": info.digest,
        "content_length": info.content_length,
        "is_public": is_public,
    }


def get_claim(digest: str, user: User, db: Db):
    claim = db.blobs.select_one(
        where=BlobClaim.digest == digest
        and (BlobClaim.user_id == user.id or BlobClaim.is_public == True)
    )
    if claim is None:
        raise HTTPException(
            status_code=404, detail=f"no blob with digest {digest} found"
        )
    return claim


@app.head("/blob/{digest}")
async def head_blob(
    digest: str,
    user: User = Depends(get_user),
    db: Db = Depends(get_db),
):
    """Get the info for a blob."""
    claim = get_claim(digest, user, db)

    assert claim.digest == digest
    # [todo] also return info like when last used, owner etc.
    return {
        "digest": claim.digest,
        "content_length": claim.content_length,
        "is_public": claim.is_public,
    }


@app.get("/blob/{digest}")
async def get_blob(
    digest: str,
    user: User = Depends(get_user),
    db: Db = Depends(get_db),
    blobstore: AbstractBlobStore = Depends(get_blobstore),
):
    """Stream the blob."""
    # [todo] feat: request ranges of blob.
    claim = get_claim(digest, user, db)
    assert claim.digest == digest

    def iterfile():
        with blobstore.open(digest) as tape:
            yield from chunked_read(tape)

    return StreamingResponse(iterfile())


@app.delete("/blob/{digest}")
async def delete_blob(
    digest: str,
    user: User = Depends(get_user),
    db: Db = Depends(get_db),
    blobstore: AbstractBlobStore = Depends(get_blobstore),
):
    with transaction():
        db.blobs.delete(
            where=BlobClaim.digest == digest and BlobClaim.user_id == user.id
        )
        if not db.blobs.has(where=BlobClaim.digest == digest):
            # [todo] race conditions?
            blobstore.delete(digest)


# Run me:
# uvicorn blobular.api.app:app --reload
