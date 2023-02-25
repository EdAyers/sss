import asyncio
from typing import Dict, Optional
import webbrowser
from aiohttp import web
import aiohttp
from .settings import Settings
from .cloudutils import  AuthenticationError
from .console import console, is_interactive_terminal, logger
import urllib.parse
from pathlib import Path

""" Code for connecting to auth server.

Todo:
    * consider removing async code, there is nothing that needs to be concurrent here.
"""


async def loopback_login(*, autoopen=True) -> str:
    """Interactive workflow to perform the github authentication loop.

    ① present a sign-in-with-github link to the user in the terminal.
    ② ping api.hitsave.io/user/login for a new JWT.
    ③ return the JWT and store it locally the JWT in a local file.

    A holder of this JWT, for the period that it is valid, is authenticated in hitsave as the person
    who logged in.
    """

    if not is_interactive_terminal():
        raise RuntimeError(
            "Can't authenticate the user in a non-interactive terminal session."
        )
    cfg = Settings.current()
    # [todo] if there is already a valid jwt, don't bother logging in here.

    redirect_port = 9449  # [todo] check not claimed.
    miniserver_url = urllib.parse.quote(f"http://127.0.0.1:{redirect_port}")
    query_params = {
        "client_id": cfg.github_client_id,  # Production GitHub OAuth app client id
        "redirect_uri": f"{cfg.web_url}/login?client_loopback={miniserver_url}",
        "scope": "user:email",
    }
    # query_params = urllib.parse.urlencode(query_params)
    query_params = "&".join(
        [f"{k}={q}" for k, q in query_params.items()]
    )  # [note] this gives slightly nicer messages over using url_parse
    base_url = "https://github.com/login/oauth/authorize"
    sign_in_url = f"{base_url}?{query_params}"
    # [todo] check user isn't already logged in
    fut = asyncio.get_running_loop().create_future()

    async def redirected(request: web.BaseRequest):
        """Handler for the mini webserver"""
        # print(request, request.url, request.method)
        ps = dict(request.url.query)
        assert "jwt" in ps, "Github redirect did not include a `jwt` param."
        # [todo] validate it's a JWT
        if not fut.done():
            fut.set_result(ps)
        return web.Response(text="Done")

    # ref: https://docs.aiohttp.org/en/stable/web_lowlevel.html
    server = web.Server(redirected)
    runner = web.ServerRunner(server)
    # [todo] add a 10s timeout
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", redirect_port)
    await site.start()
    if autoopen:
        console.print("Opening GitHub OAuth login...", sign_in_url)
        webbrowser.open_new(sign_in_url)
    else:
        console.print("Visit this url to log in:\n", sign_in_url)

    result = await fut
    await site.stop()
    await runner.cleanup()
    await server.shutdown()
    assert "jwt" in result
    jwt = result["jwt"]
    cfg.persist_jwt(jwt)
    console.print("Successfully logged in.")
    return jwt


async def generate_api_key(label: str):
    """Assuming that the user is authenticated (that is, a valid JWT is cached),
    this will generate a new hitsave api key with the given label.
    """
    cfg = Settings.current()
    jwt = cfg.get_jwt()
    cloud_url = cfg.cloud_url
    if jwt is None:
        raise AuthenticationError("User has not logged in.")

    logger.debug(f"Asking {cloud_url} for a new API key with label {label}.")
    async with aiohttp.ClientSession(
        cloud_url, headers={"Authorization": f"Bearer {jwt}"}
    ) as session:
        async with session.get("/api_key/generate", params={"label": label}) as resp:
            if resp.status == 401:
                msg = await resp.text()
                logger.debug(msg)
                raise AuthenticationError(f"Authentication session has expired.")
            resp.raise_for_status()
            if resp.content_type == "application/json":
                raise NotImplementedError(
                    "json response to /api_key/generate not implemented"
                )
            elif resp.content_type == "text/plain":
                api_key = await resp.text()
            else:
                raise Exception(f"Unknown content_type {resp.content_type}")
    logger.debug(f"Successfully recieved new API key")
    return api_key
