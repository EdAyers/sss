from datetime import datetime, timedelta
from typing import Any, NewType, Optional
from uuid import UUID
import aiohttp
from fastapi import Depends
from starlette.requests import Request
from pydantic import BaseModel, EmailStr, ValidationError
from fastapi.security.utils import get_authorization_scheme_param
from jose import ExpiredSignatureError, JWTError, jwt
from secrets import token_urlsafe
from dxd import transaction
import logging
from .settings import Settings
from .persist import ApiKey as ApiKeyEntry, User, BlobularApiDatabase as Db

logger = logging.getLogger("blobular")


class AuthenticationError(Exception):
    """Error caused by the user not being authenticated.

    That is, the user is not logged in or didn't present a valid API key."""

    pass


# [todo] use pydantic with a check


class ApiKey(BaseModel):
    value: str


class JwtClaims(BaseModel):
    sub: UUID
    """ The user id. """
    exp: datetime
    """ When the token expires. """


def from_jwt(encoded_jwt: str) -> JwtClaims:
    """Decode and validate an encoded JWT."""
    cfg = Settings.current()
    try:
        decoded = jwt.decode(
            encoded_jwt,
            key=cfg.jwt_secret.get_secret_value(),
            algorithms=[cfg.jwt_algorithm],
        )
        return JwtClaims.parse_obj(decoded)
    except ExpiredSignatureError as e:
        # [todo] if user-agent is a terminal then suggest the shell command to use.
        raise AuthenticationError("expired JWT, please log in again") from e
    except JWTError as e:
        logger.exception(e)
        raise AuthenticationError("invalid JWT") from e


def from_auth_header(s: str):
    """Decode the Authorization header's content."""
    if s.startswith("Bearer "):
        scheme, param = s.split(" ", 1)
        if scheme.lower() != "bearer":
            raise AuthenticationError(f"invalid authentication scheme")
        return from_jwt(param)
    else:
        # [todo] validation of api key here
        return ApiKey(value=s)


def from_request(request: Request):
    """Get the authentication token from a request object.

    Note that this doesn't do any security checks.
    """
    encoded_jwt = request.cookies.get("jwt")
    auth_header = request.headers.get("Authorization")
    token: ApiKey | JwtClaims
    if encoded_jwt is not None:
        token = from_jwt(encoded_jwt)
    elif auth_header is not None:
        token = from_auth_header(auth_header)
    else:
        raise AuthenticationError("no authentication token provided")
    return token


def user_of_token(token: ApiKey | JwtClaims, db: Db) -> User:
    """Takes the given token, validates it and returns the corresponding user.

    Raises:
        AuthenticationError: token is invalid.
    """
    if isinstance(token, ApiKey):
        user_id = db.api_keys.select_one(
            where=ApiKeyEntry.key == token.value, select=ApiKeyEntry.user_id
        )
        if user_id is None:
            raise AuthenticationError("unknown API key")
        user = db.users.select_one(where=User.id == user_id)
        assert user is not None, "corrupted db"
        return user
    elif isinstance(token, JwtClaims):
        user_id = token.sub
        user = db.users.select_one(where=User.id == user_id)
        if user is None:
            raise AuthenticationError(f"no such user {user_id}")
        return user
    else:
        raise AuthenticationError("unrecognized authentication token")


def get_user(request: Request, db: Db):
    """FastAPI fixture for getting the authenticated user."""
    token = from_request(request)
    user = user_of_token(token, db)
    return user
