import json
import logging
import sys
from typing import Any, Optional, Union
from pathlib import Path
import os
import subprocess
from subprocess import PIPE, CalledProcessError

""" Helpers for working with projects, config files etc. """

logger = logging.getLogger("miniscutil.config")


def get_git_root(cwd: Optional[Path] = None) -> Optional[Path]:
    """
    Gets the git root for the current working directory.

    source: https://github.com/maxnoe/python-gitpath/blob/86973f112b976a87e2ffa734fa2e43cc76dfe90d/gitpath/__init__.py
    (MIT licenced)
    """
    try:
        args = ["git", "rev-parse", "--show-toplevel"]
        logger.debug(f"Running {' '.join(args)} in {cwd or os.getcwd()}")
        r = subprocess.run(
            args,
            stdout=PIPE,
            stderr=PIPE,
            cwd=cwd,
            check=True,
        )
        if r.stdout == b"":
            return None
        return Path(r.stdout.decode().strip())
    except CalledProcessError as e:
        logger.debug("Not in a git repository:", e)
        return None


def get_workspace_dir(cwd=None) -> Optional[Path]:
    """This is our best guess to determine which folder is the developer's "workspace folder".
    This is the top level folder for the project that the developer is currently working on.

    Approaches tried:

    - for the cwd: look for a pyproject.toml
    - for the cwd: look for the git root of the cwd.

    """
    cwd = cwd or os.getcwd()

    # reference: https://github.com/python-poetry/poetry/pull/71/files#diff-e1f721c9a6040c5fbf1b5309d40a8f6e9604aa8b46469633edbc1e62da724e92
    def find(cwd, base):
        candidates = [Path(cwd), *Path(cwd).parents]
        for path in candidates:
            file = path / base
            if file.exists():
                logger.debug(f"Found a parent directory {path} with a {base}.")
                return path
        logger.debug(f"Couldn't find a {base} file for {cwd}.")
        return None

    p = find(cwd, "pyproject.toml")
    if p is not None:
        return p
    git_root = get_git_root()
    if git_root is not None:
        return Path(git_root)
    logger.debug(
        f"{cwd} is not in a git repository and no pyproject.toml could be found."
    )
    return None


def get_app_config_dir(app_name: str) -> Path:
    """Get the path to the application directory.

    The implementation is based on https://click.palletsprojects.com/en/8.1.x/api/#click.get_app_dir
    """
    if sys.platform == "win32":
        p = Path(os.environ.get("APPDATA", "~/.config"))
    elif sys.platform == "linux":
        p = Path(os.environ.get("XDG_CONFIG_HOME", "~/.config"))
    elif sys.platform == "darwin":  # macos
        # [todo] "~/Library/Application Support" or "~/Library/Preferences"?
        p = Path("~") / "Library" / "Application Support"
    else:
        p = Path(os.environ.get("XDG_CONFIG_HOME", "~/.config"))
    p = p.expanduser().resolve() / app_name
    p.mkdir(exist_ok=True)
    return p


def get_app_cache_dir(app_name: str) -> Path:
    if sys.platform == "win32":
        p = Path(os.environ.get("LOCALAPPDATA", "~/.cache"))
    elif sys.platform == "linux":
        p = Path(os.environ.get("XDG_CACHE_HOME", "~/.cache"))
    elif sys.platform == "darwin":  # macos
        p = Path("~/Library/Caches")
    else:
        logger.warning(
            f"Unrecognised platform: {sys.platform}, user cache is defaulting to a tmpdir."
        )
        p = Path(tempfile.gettempdir())
    p = p.expanduser().resolve() / app_name
    p.mkdir(exist_ok=True)
    return p


def persist_config(p: Path, key: Union[str, tuple[str, ...]], value: Any):
    if p.exists():
        root = json.loads(p.read_text())
    else:
        root = {}
    if isinstance(key, str):
        key = (key,)
    j = root
    for k in key[:-1]:
        if k not in j:
            j[k] = {}
        j = j[k]
    j[key[-1]] = value
    p.write_text(json.dumps(root, indent=2))


def get_config(p: Path, key: Union[str, tuple[str, ...]]) -> Any:
    if not p.exists():
        raise LookupError(f"{p} does not exist")
    j = json.loads(p.read_text())
    if isinstance(key, str):
        key = (key,)
    for k in key:
        j = j[k]
    return j
