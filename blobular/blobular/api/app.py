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


# Run me:
# uvicorn blobular.api.app:app --reload

if __name__ == "__main__":
    import uvicorn

    cfg = Settings.current()

    uvicorn.run(app, host="0.0.0.0", port=3000)

# https://github.com/login/oauth/authorize?client_id=b7d5bad7787df04921e7&scope=user:email&redirect_uri=http://127.0.0.1:3000/login%3Fclient_loopback%3Dhttp/3A/127.0.0.1/3A9449
