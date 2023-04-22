import json
import logging
import sys
from typing import Any, Optional, Union
from pathlib import Path
import os
import subprocess
from subprocess import PIPE, CalledProcessError
from miniscutil.type_util import is_subtype

from pydantic import BaseSettings, Field, SecretStr

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
    """Returns the path that ths OS wants you to use to place application-specific caching files."""
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


class SecretPersist:
    """Mixin for managing secrets that live in a config file.

    ## Worked example:

    ```python
    from pydantic import BaseSettings
    from miniscutil import SecretPersist, get_app_config_dir

    CONFIG_DIR = get_app_config_dir("my_app")

    class Settings(BaseSettings, SecretPersist):
        api_key: Optional[SecretStr] = Field(default=None, is_secret=True)

        def get_api_key(self):
            return self.get_secret("api_key")

        def persist_api_key(self, api_key: str):
            self.persist_secret("api_key", api_key)

        @property
        def secrets_file(self) -> Path:
            return CONFIG_DIR / "secrets.json"

        class Config:
            env_file = ".env"
            env_file_encoding = "utf-8"


    if __name__ == "__main__":
       cfg = Settings()
       k1 = cfg.get_secret('api_key')
       print(k1)
       cfg.persist_secret('api_key', 'asdf')
       k2 = cfg.get_secret('api_key')
       print(k2)

    ```

    Note that if you want to access the secret, you should use `cfg.get_secret` instead of `cfg.api_key`.
    The field will still be accessable but if the secret is stored in a config file then it won't automatically access it.
    Once you have called 'get_secret' the value of `cfg.api_key` will be updated.

    Note also that this means that if there is `api_key` present as an environment variable, this __takes precedence__
    over the `api_key` field in the secrets file.

    ## Adding multiple secrets for the same key but different configurations

    Often you want to be able to store multiple secret values for different configurations.
    For example you might need a different API key for dev/prod servers.

    You can set this up by adding `secret_postfix` field to the pydantic config:

    ```
    class Settings(BaseSettings, SecretPersist):
        mode : Literal['dev', 'prod'] = Field(default='dev')
        ...
        class Config:
            ...
            secret_postfix = lambda self: self.mode
    ```

    Now, when you call `persist_secret`, it will save it in a different slot determined by
    `secret_postfix`. So now `get_secret`'s return value will depend on whether mode is dev or prod.

    ## Todos

    - [ ] add a warning if the secrets file is overridden
    - [ ] automatically get the secret from the secret file on field access.

    """

    def _get_secret_prelude(self, key: str):
        assert isinstance(self, BaseSettings)
        fields = getattr(self, "__fields__")
        assert key in fields
        field = fields[key]
        assert is_subtype(SecretStr, field.annotation)
        extra = field.field_info.extra
        assert extra.get(
            "is_secret", False
        ), "please add the 'is_secret=True' kwarg to Field constructor"
        cfg = getattr(self, "__config__")
        secrets_file = getattr(self, "secrets_file")
        secret_postfix = getattr(cfg, "secret_postfix", None)
        if secret_postfix is None:
            secret_postfix = "default"
        elif secret_postfix in fields:
            secret_postfix = getattr(self, secret_postfix)
            assert isinstance(secret_postfix, str)
        elif callable(secret_postfix):
            secret_postfix = secret_postfix(self)
        else:
            raise ValueError(f"secret_postfix {secret_postfix} not found")
        return secrets_file, secret_postfix

    def persist_secret(self, key: str, secret: str):
        file, postfix = self._get_secret_prelude(key)
        logger.debug(f"Saving secret {key} for {postfix} to {file}")
        persist_config(file, (key, postfix), secret)
        setattr(self, key, SecretStr(secret))

    def get_secret(self, key: str) -> Optional[str]:
        file, postfix = self._get_secret_prelude(key)
        secret: Optional[SecretStr] = getattr(self, key, None)
        if secret is None:
            try:
                value = get_config(file, (key, postfix))
                if value is None:
                    logger.debug(f"{key} for {postfix} was invalidated")
                    return None
            except LookupError:
                logger.debug(f"no {key} for {postfix} found in {file}")
                return None
            setattr(self, key, SecretStr(value))
            return value
        else:
            return secret.get_secret_value()

    def invalidate_secret(self, key: str):
        file, postfix = self._get_secret_prelude(key)
        persist_config(file, (key, postfix), None)
        setattr(self, key, None)
