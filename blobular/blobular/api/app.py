from dataclasses import replace
from datetime import datetime
from functools import partial
import tempfile
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse
from blobular.util import human_size
from fastapi import FastAPI
from secrets import token_urlsafe
from typing import Optional
import logging
from uuid import UUID
from fastapi import Depends, FastAPI, HTTPException, UploadFile
from fastapi.responses import StreamingResponse, PlainTextResponse, RedirectResponse
from miniscutil.misc import append_url_params
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
    database,
)
from blobular.store import (
    AbstractBlobStore,
    get_digest_and_length,
    OnDatabaseBlobStore,
    BlobContent,
)
from miniscutil import chunked_read
from blobular.api.authentication import get_user
from dxd import transaction, engine_context
from blobular.api.api import router as api_router
from blobular.api.web import router as web_router
from pathlib import Path
import boto3

app = FastAPI()
app.include_router(api_router)
app.include_router(web_router)

logger = logging.getLogger("blobular")


@app.on_event("startup")
def startup_event():
    database.connect()


@app.on_event("shutdown")
def shutdown_event():
    database.disconnect()


@app.exception_handler(AuthenticationError)
def _auth_err(request, exc: AuthenticationError):
    return PlainTextResponse(str(exc), status_code=401)


@app.exception_handler(Exception)
def _any_err(request, exc: Exception):
    logger.exception("internal blobular error")
    return PlainTextResponse("internal blobular error", status_code=500)


@app.get("/login")
async def web_login(
    request: Request,
    code: str,
    redirect_uri: Optional[str] = None,
    state: Optional[str] = None,
    db=Depends(database),
):
    """Login handler for github.

    To use, send a request to https://github.com/login/oauth/authorize
    with client_id, redirect_uri and scope set correctly.

    """
    cfg = Settings.current()
    jwt = await login_handler(code, db)
    max_age = int(cfg.jwt_expires.total_seconds())
    domain = urlparse(cfg.cloud_url).netloc
    headers = {"Set-Cookie": f"jwt={jwt}; HttpOnly; Max-Age={max_age}; domain={domain}"}

    if redirect_uri is None:
        # [todo] allow redirects to other routes in our domain
        # remember: never allow arbitrary redirects to other domains
        # for now just always redirect to index.
        return RedirectResponse("/", headers=headers)

    redirect_domain = urlparse(redirect_uri).netloc
    if redirect_domain == domain:
        path = urlparse(redirect_uri).path
        return RedirectResponse(path, headers=headers)
    elif redirect_domain.startswith("127.0.0.1"):
        # local redirect for client loopback
        redirect_uri = append_url_params(redirect_uri, jwt=jwt)
        headers.update(
            {
                "Access-Control-Allow-Origin": "http://127.0.0.1",
                "Access-Control-Allow-Methods": "GET, OPTIONS",
                "Access-Control-Allow-Headers": "Origin, X-Requested-With, Content-Type, Accept",
            }
        )
        return RedirectResponse(redirect_uri, headers=headers)
    else:
        raise HTTPException(400, "invalid redirect_uri")


# run in dev:
# uvicorn blobular.api.app:app --reload

# run in prod:
# gunicorn -c gunicorn_conf.py blobular.api.app:app

if __name__ == "__main__":
    import uvicorn

    cfg = Settings.current()

    uvicorn.run(app, host="0.0.0.0", port=3000)

# https://github.com/login/oauth/authorize?client_id=b7d5bad7787df04921e7&scope=user:email&redirect_uri=http://127.0.0.1:3000/login%3Fclient_loopback%3Dhttp/3A/127.0.0.1/3A9449
