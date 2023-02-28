from io import BufferedReader
import io
from pathlib import Path
from typing import IO, Any, Iterable, Iterator, Literal, Optional
import logging
import json
import warnings
from .settings import Settings
from miniscutil import chunked_read, human_size
import requests
from urllib3.exceptions import NewConnectionError
from rich import print

logger = logging.getLogger("blobular")


class AuthenticationError(Exception):
    """Raised when the user is not authenticated.

    That is, the JWT or API key is nonexistent or not valid."""


already_reported_connection_error = False
""" This is true to only report a bad connection as a warning once. """


def get_server_status():
    r = request("GET", "/status", no_auth_ok=True)
    if r.ok:
        return r.json()
    r.raise_for_status()




def print_jwt_status() -> bool:
    """Returns true if we are authenticated with a JWT auth."""
    cfg = Settings.current()
    jwt = cfg.get_jwt()
    if jwt is None:
        print("not logged in")
        return False
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Cache-Control": "no-cache",
    }
    response = request("GET", "/user", headers=headers)
    if response.ok:
        user = response.json()
        id = user.get("id")
        gh_username = user.get("gh_username")
        print("signed in")
        return True
    if response.status_code == 401:
        cfg.invalidate_jwt()
        print(f"not logged in: {response.text}")
        return False
    if response.status_code == 403:
        cfg.invalidate_jwt()
        print(f"forbidden: {response.text}")
        return False
    response.raise_for_status()
    raise NotImplementedError(response)


def print_api_key_status() -> None:
    cfg = Settings.current()
    api_key = cfg.get_api_key()
    if api_key is None:
        print("no API key found")
        return
    response = request("GET", "/user", headers={"Cache-Control": "no-cache"})
    if response.status_code == 200:
        print("API key valid")
    elif response.status_code == 401:
        reason = response.text
        print(f"API key not valid: {reason}")
    else:
        print("unknown response", response.status_code, response.text)
        response.raise_for_status()


def request(
    method: str, path, headers: dict[str, str] = {}, no_auth_ok=False, **kwargs
) -> requests.Response:
    """Sends an HTTP request to the api, we provide the right authentication headers.

    Uses the same signature as ``requests.request``.
    You can perform a streaming upload by passing an Iterable[bytes] as the ``data`` argument.

    Reference: https://requests.readthedocs.io/en/latest/user/advanced/#streaming-uploads

    Raises:
      ConnectionError: if we can't connect to the cloud.
      AuthenticationError: if no_auth_ok is False and we can't find an authentication method.
    """
    global already_reported_connection_error
    cfg = Settings.current()
    if "Authorization" not in headers:
        api_key = cfg.get_api_key()
        if api_key is None:
            if not no_auth_ok:
                raise AuthenticationError(
                    "no API key or authentication header found to authenticate"
                )
        else:
            headers = {"Authorization": api_key, **headers}
    cloud_url = cfg.cloud_url
    try:
        r = requests.request(method, cloud_url + path, **kwargs, headers=headers)
        return r
    except (requests.exceptions.ConnectionError, NewConnectionError) as err:
        if not already_reported_connection_error:
            logger.warning(f"could not reach {cloud_url}. Using in offline mode.")
            already_reported_connection_error = True
        raise ConnectionError from err
