from dataclasses import asdict, dataclass, field, fields, replace
import logging
import re
import tempfile
import os
import os.path
import sys
from pathlib import Path
from typing import Literal, Optional, Type, TypeVar, Any, List, Dict, Iterable
from miniscutil import Current
import importlib.metadata
from subprocess import PIPE, check_output, CalledProcessError
import io
import os
import subprocess
from functools import cache
from contextlib import redirect_stderr

from miniscutil import (
    get_git_root,
    get_app_config_dir,
    get_workspace_dir,
    get_app_cache_dir,
)
from miniscutil.config import SecretPersist

from pydantic import BaseSettings, Field, SecretStr
from hitsave.__about__ import __version__

app_name = "hitsave"

logger = logging.getLogger(app_name)


class Settings(BaseSettings, Current, SecretPersist):
    web_url: str = Field("https://hitsave.io")
    """ URL for the HitSave website. """
    cloud_url: str = Field("https://api.hitsave.io")
    """ URL for hitsave cloud API server. """
    github_client_id: str = Field(
        "a569cafe591e507b13ca"
    )  # [todo] get this from cloud_url
    """ This is the github client id used to authenticate the app. """
    no_advert: bool = Field(True)
    """ If this is true then we won't bother you with a little advert for signing up to hitsave.io on exit. """

    version_sensitivity: Literal["none", "minor", "major", "patch"] = Field("minor")
    """ This is the sensitivity the digest algorithm should have to the versions of external packages.
    So if ``version_sensitivity = 'minor'``, then upgrading a package from ``3.2.0`` to ``3.2.1`` won't invalidate the cache,
    but upgrading to ``3.3.0`` will. Non-standard versioning schemes will always invalidate unless in 'none' mode.
    """
    local_cache_dir: Path = Field(default_factory=lambda: get_app_cache_dir(app_name))
    """ This is the directory where hitsave should store local caches of data. """

    workspace_dir: Path = Field(default_factory=get_workspace_dir)
    """ Directory for the current project, should be the same as workspace_folder in vscode.
    It defaults to the nearest parent folder containing pyproject.toml or git root. """

    config_dir: Path = Field(default_factory=lambda: get_app_config_dir(app_name))

    api_key: Optional[SecretStr] = Field(None, is_secret=True)
    jwt: Optional[SecretStr] = Field(None, is_secret=True)

    class Config:
        env_prefix = app_name + "_"
        env_file = ".env"
        env_file_encoding = "utf-8"
        secret_postfix = lambda self: self.cloud_url

    @property
    def local_db_path(self) -> Path:
        """Gets the path to the local sqlite db that is used to store local state."""
        return self.local_cache_dir / "localstore.db"

    @property
    def api_key_file_path(self) -> Path:
        """Gets the path of the local file that contains the API keys of the application.
        Keys are stored as tab-separated (url, key) pairs.
        """
        return self.config_dir / "api_keys.txt"

    @property
    def project_config_path(self):
        return self.workspace_dir / f"{app_name}.conf"

    @property
    def secrets_file(self) -> Path:
        return self.config_dir / "secrets.json"

    @classmethod
    def default(cls):
        return cls()  # type: ignore
