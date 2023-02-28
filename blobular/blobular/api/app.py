from dataclasses import replace
from datetime import datetime
from functools import partial
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse
from blobular.util import human_size
from fastapi import FastAPI
from secrets import token_urlsafe
from typing import Optional
import logging
from uuid import UUID
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel, SecretStr
from starlette.requests import Request
from starlette.datastructures import URL
from blobular.api.settings import Settings
from blobular.store.s3 import S3BlobStore
from blobular.store.cache import SizedBlobStore
from blobular.store.localfile import LocalFileBlobStore
from blobular.__about__ import __version__

from blobular.registry import BlobClaim
from blobular.api.authentication import (
    AuthenticationError,
    JwtClaims,
    from_request,
    user_of_token,
)
from blobular.api.github_login import login_handler

from blobular.api.persist import (
    BlobularApiDatabase as Db,
    User,
    ApiKey as ApiKeyEntry,
)
from blobular.store import (
    AbstractBlobStore,
    get_digest_and_length,
    OnDatabaseBlobStore,
    BlobContent,
)
from miniscutil import chunked_read
from blobular.api.authentication import get_user as get_user_core
from dxd import transaction
from pathlib import Path
import boto3

app = FastAPI()
logger = logging.getLogger("blobular")

db = Db()

get_user = partial(get_user_core, db=db)


@app.on_event("startup")
def startup_event():
    db.connect()


@app.on_event("shutdown")
def shutdown_event():
    db.disconnect()


@app.exception_handler(AuthenticationError)
def _auth_err(request, exc: AuthenticationError):
    return PlainTextResponse(str(exc), status_code=401)


@app.exception_handler(Exception)
def _any_err(request, exc: Exception):
    logger.exception("internal blobular error")
    return PlainTextResponse("internal blobular error", status_code=500)


@app.get("/user")
async def handle_get_user(user: User = Depends(get_user)):
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


@app.get("/status")
async def handle_get_status():
    """Get the status of the server."""
    return {
        "status": "ok",
        "time": datetime.utcnow().isoformat(),
        "version": __version__,
    }


def append_url_params(url: str, params: dict):
    # https://stackoverflow.com/questions/2506379/add-params-to-given-url-in-python
    parts = urlparse(url)
    query = dict(parse_qsl(parts.query))
    query.update(params)
    parts = parts._replace(query=urlencode(query))
    return urlunparse(parts)


@app.get("/auth/github/client_id")
def get_github_client_id():
    """Gets the github client id that we use for authentication."""
    # [todo] there will be an official OAuth way of saying what authentication providers we support
    return PlainTextResponse(Settings.current().github_client_id)


@app.get("/auth/github/login")
async def login(
    request: Request,
    code: str,
    state: str | None = None,
    client_loopback: Optional[str] = None,
):
    """Login to the server. The code param should be a github authentication code.


    Args:
        code: GitHub authentication code.
        state: GitHub authentication state (optional).
        client_loopback: This means that the login was initiated by a Python client and gives the address to loop back to.
    Todo:
        * Make this work with general OAuth2

    """
    cfg = Settings.current()
    jwt = await login_handler(code, db)
    max_age = cfg.jwt_expires.seconds
    domain = cfg.cloud_url

    headers = {
        "Set-Cookie": f"jwt={jwt}; HttpOnly; Max-Age={max_age}; domain={domain}",
        # [todo] are these CORS headers needed?
        "Access-Control-Allow-Origin": "http://127.0.0.1",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept",
    }
    if client_loopback is not None:
        url = append_url_params(client_loopback, {"jwt": jwt})
        return RedirectResponse(url, headers=headers)
    else:
        # this case is when they login from a browser.
        return PlainTextResponse(jwt, headers=headers)


@app.post("/api_key/generate")
def generate_api_key(request: Request):
    token = from_request(request)
    if not isinstance(token, JwtClaims):
        raise AuthenticationError(
            "you must be authenticated with a JWT to create an API key"
        )
    user = user_of_token(token, db)
    key = "hs-" + token_urlsafe(16)
    db.api_keys.insert_one(ApiKeyEntry(key=key, user_id=user.id))
    return PlainTextResponse(key)


@app.get("/blob")
async def get_blobs(user=Depends(get_user)):
    blobs = list(db.blobs.select(where=BlobClaim.user_id == user.id))
    return {"blobs": blobs}


@app.put("/blob")
async def put_blob(
    file: UploadFile,  # [todo] make optional, so you can change settings on a blob without uploading.
    is_public: bool = False,
    label: Optional[str] = None,
    user=Depends(get_user),
):
    """Upload a blob."""
    # [todo] raise if they go over quota
    # [todo] api to upload parts
    # [todo] enforce each upload part is no larger than 100MB
    info = db.blobstore.add(file.file)
    # [todo] perform in single query
    with transaction(db.engine):
        where = (BlobClaim.user_id == user.id) & (BlobClaim.digest == info.digest)
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
        where=(BlobClaim.digest == digest)
        & ((BlobClaim.user_id == user.id) | (BlobClaim.is_public == True))
    )
    if claim is None:
        raise HTTPException(
            status_code=404, detail=f"no blob with digest {digest} found"
        )
    assert claim.digest == digest, "oops"
    return claim


@app.head("/blob/{digest}")
async def head_blob(
    digest: str,
    user: User = Depends(get_user),
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
):
    """Stream the blob."""
    # [todo] feat: request ranges of blob.
    claim = get_claim(digest, user, db)
    assert claim.digest == digest

    def iterfile():
        with db.blobstore.open(digest) as tape:
            yield from chunked_read(tape)

    return StreamingResponse(iterfile())


@app.delete("/blob/{digest}")
async def delete_blob(
    digest: str,
    user: User = Depends(get_user),
):
    with transaction():
        db.blobs.delete(
            where=BlobClaim.digest == digest and BlobClaim.user_id == user.id
        )
        if not db.blobs.has(where=BlobClaim.digest == digest):
            # [todo] race conditions?
            db.blobstore.delete(digest)


# Run me:
# uvicorn blobular.api.app:app --reload

if __name__ == "__main__":
    import uvicorn

    cfg = Settings.current()

    uvicorn.run(app, host="0.0.0.0", port=3000)

# https://github.com/login/oauth/authorize?client_id=b7d5bad7787df04921e7&scope=user:email&redirect_uri=http://127.0.0.1:3000/login%3Fclient_loopback%3Dhttp/3A/127.0.0.1/3A9449
