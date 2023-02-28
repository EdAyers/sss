from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3
import sys
from typing import Any, Dict, Optional
from miniscutil.misc import chunked_read, human_size
import typer
from dataclasses import dataclass, fields, asdict
import asyncio
import platform
from enum import Enum
from blobular.cli.console import tape_progress
from blobular.cli.state import AppState
from blobular.cli.login import (
    AuthenticationError,
    generate_api_key,
    loopback_login,
)
from blobular.cli.cloudutils import (
    get_server_status,
    print_api_key_status,
    print_jwt_status,
    request,
)
from blobular.cli.console import (
    console,
    decorate,
    logger,
    user_info,
    is_interactive_terminal,
)
from blobular.cli.settings import Settings, APP_NAME
from blobular.cli.filesnap import DirectorySnapshot, FileSnapshot
from rich.prompt import Confirm

from blobular.__about__ import __version__ as version

from blobular.store.cloud import CloudBlobStore

from miniscutil import Current
from dxd import Table, engine_context
from dxd.sqlite_engine import SqliteEngine


app = typer.Typer()
""" Entrypoint for CLI tool. """


def version_callback(value: bool):
    if value:
        typer.echo(version)
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the version and exit.",
    )
):
    return


@app.command()
def login(
    no_autoopen: bool = typer.Option(
        False,
        "--no-autoopen",
    ),
):
    f"""Log in or sign up to the {APP_NAME} cloud service.

    This will present a link to you which can be used to register a {APP_NAME} account using your github account.
    """
    autoopen = not no_autoopen
    asyncio.run(loopback_login(autoopen=autoopen))


async def keygen_async():
    """Interactive workflow for generating a new api key."""
    cfg = Settings.current()
    if cfg.api_key is not None:
        console.print(f"An API key for hitsave is already present.")
        if is_interactive_terminal():
            r = Confirm.ask("Do you wish to generate another API key?")
            if not r:
                return
    label = platform.node()

    async def login():
        if is_interactive_terminal():
            await loopback_login()
        else:
            raise AuthenticationError(
                "Please login by running `hitsave login` in an interactive terminal."
            )

    if cfg.get_jwt() is None:
        await login()
    try:
        api_key = await generate_api_key(label)
    except AuthenticationError as err:
        console.print("Authentication session expired, please log in again:")
        await login()
        api_key = await generate_api_key(label)

    if not is_interactive_terminal():
        # if a human is not viewing the terminal, it should just print
        # api_key on stdout and exit.
        print(api_key)
        return
    console.print(
        f"API keys are used to provide programmatic access to the {APP_NAME} cloud API.\n",
        "This API key should be stored in a secret location and not shared, as anybody\ncan use it to authenticate as you.",
        "\n\n",
        f"[green bold]{api_key}[/]" "\n",
        sep="",
    )
    console.print(f"Saving key to {cfg.secrets_file}.")
    cfg.persist_api_key(api_key)


@app.command()
def keygen(label: Optional[str] = None):
    """Generate a fresh API key.

    If the current shell is zsh, you will also be asked whether you
    want to append the API key to your .zshenv file.
    """
    asyncio.run(keygen_async())


@app.command()
def add(path: Path = typer.Argument(..., exists=True)):
    """Upload the given file or directory to the cloud, returning a digest that can be used to reference data in code."""
    if path.is_file():
        snap = FileSnapshot.snap(path)
        snap.upload()
        print(snap.digest)
    elif path.is_dir():
        snap = DirectorySnapshot.snap(path)
        snap.upload()
        print(snap.digest)
    else:
        raise ValueError(f"Can't snapshot {path}.")


@app.command()
def open(digest: str, path: Optional[Path] = None):
    # [todo] if it exists in the filestore, just symlink it.
    s = AppState.current()
    head = s.store.get_info(digest)
    if head is None:
        logger.error("digest not found")
        return
    s.store.pull(digest)
    if path is not None:
        g = path.open("wb")
    else:
        if sys.stdout.isatty():
            logger.error(
                f"refusing to dump to terminal stdout, please specify a path or pipe the output."
            )
            return
        g = nullcontext(sys.stdout.buffer)
    try:
        with g as g:
            with s.store.open(digest) as f:
                for b in chunked_read(f):
                    g.write(b)

    except LookupError:
        logger.error("digest not found")


def print_user_status():
    r = request("GET", "/user")
    r.raise_for_status()
    r = r.json()
    gh_username = r.get("gh_username")
    if gh_username is not None:
        print(f"logged in as https://github.com/{gh_username}")
    else:
        print(f"logged in with user id {id}")
    usage = r.get("usage", 0)
    quota = r.get("quota")
    if quota is not None:
        print(
            f"usage: {human_size(usage)} out of {human_size(quota)} ({usage / quota * 100:.2f}%)"
        )
    else:
        print(f"usage: {human_size(usage)}")


@app.command()
def status():
    """Prints details of the connection."""
    cfg = Settings.current()
    print(f"server: {cfg.cloud_url}")
    print(f"workspace: {cfg.workspace_dir}")
    try:
        print_jwt_status()
        print_api_key_status()
        status = get_server_status()
        assert isinstance(status, dict)
        print(f'server version: {status.get("version")}')
        print(f"client version: {version}")
        print_user_status()

    except ConnectionError as e:
        print(f"not connected")
    # [todo] info about local cache for the given project.


@app.command()
def clear_local():
    """Delete all local blob data"""
    a = AppState.current()
    a.store.clear_cache()


if __name__ == "__main__":
    app()
