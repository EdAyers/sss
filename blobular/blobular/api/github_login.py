from datetime import datetime, timedelta
from secrets import token_urlsafe
from typing import Any, Optional
from uuid import UUID
import aiohttp
from dxd import transaction
from fastapi import Depends
from pydantic import BaseModel, EmailStr

from .settings import Settings
from .persist import User, BlobularApiDatabase as Db
from jose import ExpiredSignatureError, JWTError, jwt


class LoginError(Exception):
    """Exception thrown when a login attempt fails"""

    pass


class NoPrimaryEmail(LoginError):
    pass


class UserInfoNotAvailable(LoginError):
    pass


class GithubAccessTokenResponse(BaseModel):
    access_token: str


class GithubUserInfo(BaseModel):
    id: int
    login: str
    avatar_url: str


class GithubEmail(BaseModel):
    email: EmailStr
    verified: bool
    primary: bool


def generate_jwt(user_uuid: UUID, expires_delta: Optional[timedelta] = None):
    cfg = Settings.current()
    expires_delta = expires_delta or cfg.jwt_expires
    assert isinstance(expires_delta, timedelta)
    try:
        expire = datetime.utcnow() + expires_delta
        claims = {
            "exp": expire,
            "sub": user_uuid.hex,
            "iat": datetime.utcnow(),
            "iss": cfg.cloud_url,
            "jti": token_urlsafe(16),
        }
        encoded_jwt = jwt.encode(
            claims=claims,
            key=cfg.jwt_secret.get_secret_value(),
            algorithm=cfg.jwt_algorithm,
        )
        return encoded_jwt
    except JWTError as e:
        raise LoginError("error encoding JWT") from e


async def get_github_access_token(code: str):
    cfg = Settings.current()
    try:
        async with aiohttp.ClientSession() as session:
            headers = {"Accept": "application/json"}
            params: dict[str, str] = {
                "client_id": cfg.github_client_id,
                "client_secret": cfg.github_client_secret.get_secret_value(),
                "code": code,
            }
            async with session.get(
                "https://github.com/login/oauth/access_token",
                headers=headers,
                params=params,
            ) as resp:
                resp.raise_for_status()
                gr = GithubAccessTokenResponse.parse_obj(await resp.json())
                return gr.access_token
    except aiohttp.ClientResponseError as e:
        raise LoginError("error retrieving GitHub access token") from e


async def get_github_user_info(access_token: str):
    cfg = Settings.current()
    headers = {
        "Accept": "application/json",
        "User-Agent": cfg.github_user_agent,
        "Authorization": f"Bearer {access_token}",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get("https://api.github.com/user") as resp:
                resp.raise_for_status()
                user_info = GithubUserInfo.parse_obj(await resp.json())
            async with session.get("https://api.github.com/user/emails") as resp:
                resp.raise_for_status()
                emails = [GithubEmail.parse_obj(j) for j in await resp.json()]
            return user_info, emails
    except aiohttp.ClientResponseError as e:
        raise LoginError() from e


async def login_handler(code: str, db) -> str:
    """FastAPI handler for GitHub login.

    Using the authorization code provided by GitHub, this function will
    get user information on GitHub, make a new User record in our DB and
    return the signed JWT.

    Args:
        code (str): The authorization code from GitHub.
    Returns:
        A string of the signed JWT to issue to the login requester.
    Raises:
        AuthenticationError: If the request fails for any reason.
    """
    access_token = await get_github_access_token(code)
    user_info, emails = await get_github_user_info(access_token)

    primary_email = next((email for email in emails if email.primary), None)
    if primary_email is None:
        raise NoPrimaryEmail(
            "we only allow logins from GitHub users with a primary email."
        )
    # [todo] block unverified email?
    upsert: Any = dict(
        gh_id=user_info.id,
        gh_email=str(primary_email.email),
        gh_username=user_info.login,
        gh_avatar_url=user_info.avatar_url,
        email_verified=primary_email.verified,
    )
    with transaction(db.engine):
        # [todo] write Table.upsert
        user = db.users.select_one(where=User.gh_id == user_info.id)
        if user is None:
            user = User(**upsert)
            db.users.insert_one(user)
        else:
            i = db.users.update(upsert, where=User.id == user.id)
            assert i == 1, "corrupted db"

    encoded_jwt = generate_jwt(user.id)
    return encoded_jwt
