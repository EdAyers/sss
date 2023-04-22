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
from .settings import Settings
from blobular.store.s3 import S3BlobStore
from blobular.store.cache import SizedBlobStore
from blobular.store.localfile import LocalFileBlobStore
from blobular.__about__ import __version__
from rich.logging import RichHandler

from .authentication import (
    AuthenticationError,
    JwtClaims,
    from_request,
    user_of_token,
)
from .github_login import login_handler

from .persist import (
    BlobularApiDatabase as Db,
    User,
    ApiKey as ApiKeyEntry,
    database,
)
from miniscutil import chunked_read
from .authentication import get_user
from dxd import transaction, engine_context
from .api import router as api_router
from .web import router as web_router
from pathlib import Path

app = FastAPI()
app.include_router(api_router)
app.include_router(web_router)

logger = logging.getLogger(__name__)


@app.on_event("startup")
def startup_event():
    database.connect()


@app.on_event("startup")
async def setup_loggers():
    logger = logging.getLogger("server")
    logger.setLevel(logging.DEBUG)  # [todo] if in dev mode
    # handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(RichHandler())


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
    domain = urlparse(cfg.cloud_url).hostname
    headers = {"Set-Cookie": f"jwt={jwt}; HttpOnly; Max-Age={max_age}; domain={domain}"}

    if redirect_uri is None:
        # [todo] allow redirects to other routes in our domain
        # remember: never allow arbitrary redirects to other domains
        # for now just always redirect to index.
        logger.debug(f'redirect_uri is None')
        return RedirectResponse("/", headers=headers)

    redirect_domain = urlparse(redirect_uri)
    # note: `.netloc` includes port, `.hostname` does not.
    if redirect_domain.hostname == domain:
        logger.debug(f'login redirect exact match')
        path = urlparse(redirect_uri).path
        return RedirectResponse(path, headers=headers)
    elif redirect_domain.hostname == "127.0.0.1":
        # local redirect for client loopback
        logger.debug(f'login redirect is a localhost {redirect_domain}')
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
