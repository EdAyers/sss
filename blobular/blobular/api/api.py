from datetime import datetime
import tempfile
from typing import Optional
from blobular.api.persist import User
from blobular.registry import BlobClaim
from blobular.store import get_digest_and_length
from dxd import transaction
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from miniscutil import human_size, chunked_read
from blobular.api.persist import User, database, BlobularApiDatabase as Db
from blobular.api.authentication import get_user, router as auth_router
from blobular.__about__ import __version__

router = APIRouter(prefix="/api")
router.include_router(auth_router)


@router.get("/user")
async def handle_get_user(user: User = Depends(get_user), db=Depends(database)):
    """Get the current user."""
    usage = int(
        db.blobs.sum(BlobClaim.content_length, where=BlobClaim.user_id == user.id)
    )
    return {
        "gh_id": user.gh_id,
        "gh_avatar_url": user.gh_avatar_url,
        "gh_username": user.gh_username,
        "id": user.id,
        "usage": usage,
        "quota": user.quota,
        "usage_h": human_size(usage),
    }


@router.get("/status")
async def handle_get_status():
    """Get the status of the server."""
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "version": __version__,
    }


@router.get("/blob")
async def get_blobs(user=Depends(get_user), db=Depends(database)):
    blobs = list(db.blobs.select(where=BlobClaim.user_id == user.id))
    return {"blobs": blobs}


@router.put("/blob/{digest}")
async def put_blob(
    request: Request,  # [todo] make optional, so you can change settings on a blob without uploading.
    digest: str,
    is_public: bool = False,
    label: Optional[str] = None,
    user=Depends(get_user),
    db=Depends(database),
):
    """Upload a blob. The request body is the raw blob data."""
    # [todo] raise if they go over quota
    # [todo] api to upload parts
    # [todo] enforce each upload part is no larger than 100MB
    # [todo] handle Expect: 100-Continue
    # [todo] Content-Length should be set
    with tempfile.SpooledTemporaryFile() as f:
        async for chunk in request.stream():
            f.write(chunk)
        f.seek(0)
        actual_digest, content_length = get_digest_and_length(f)
        f.seek(0)
        if actual_digest != digest:
            raise HTTPException(
                status_code=400, detail=f"digest mismatch: I got {actual_digest}"
            )
        info = db.blobstore.add(f, digest=actual_digest, content_length=content_length)

    # [todo] perform in single query
    with transaction(db.engine):
        where = (BlobClaim.user_id == user.id) & (BlobClaim.digest == actual_digest)
        b = db.blobs.select_one(where=where)
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
            assert b.content_length == info.content_length
            if is_public and not b.is_public:
                db.blobs.update(
                    where=where,
                    values={"is_public": True},
                )
            if b.is_public:
                is_public = True
    return {
        "created": b is None,
        "digest": info.digest,
        "content_length": info.content_length,
        "is_public": is_public,
    }


def get_claim(digest: str, user: User, db: Db = Depends(database)):
    claim = db.blobs.select_one(
        where=(BlobClaim.digest == digest)
        & ((BlobClaim.user_id == user.id) | (BlobClaim.is_public == True))
    )
    if claim is None:
        raise HTTPException(
            status_code=404, detail=f"no blob with digest {digest} found"
        )
    assert claim.digest == digest, "oops"
    return claim


def touch(digest: str, user: User, db: Db):
    db.blobs.update(
        {
            BlobClaim.last_accessed: datetime.utcnow(),
            BlobClaim.accesses: BlobClaim.accesses + 1,
        },
        where=(BlobClaim.digest == digest)
        & ((BlobClaim.user_id == user.id) | (BlobClaim.is_public == True)),
    )


@router.get("/blob/{digest}/info")
async def head_blob(digest: str, user: User = Depends(get_user), db=Depends(database)):
    """Get the info for a blob."""
    claim = get_claim(digest, user, db)

    assert claim.digest == digest
    # [todo] also return info like when last used, owner etc.
    return {
        "digest": claim.digest,
        "content_length": claim.content_length,
        "is_public": claim.is_public,
    }


@router.get("/blob/{digest}")
async def get_blob(digest: str, user: User = Depends(get_user), db=Depends(database)):
    """Stream the blob."""
    # [todo] feat: request ranges of blob.
    claim = get_claim(digest, user, db)
    touch(digest, user, db)
    assert claim.digest == digest

    def iterfile():
        with db.blobstore.open(digest) as tape:
            yield from chunked_read(tape, block_size=2**10)

    return StreamingResponse(iterfile())


@router.delete("/blob/{digest}")
async def delete_blob(
    digest: str, user: User = Depends(get_user), db=Depends(database)
):
    with transaction():
        db.blobs.delete(
            where=BlobClaim.digest == digest and BlobClaim.user_id == user.id
        )
        if not db.blobs.has(where=BlobClaim.digest == digest):
            # [todo] race conditions?
            db.blobstore.delete(digest)
