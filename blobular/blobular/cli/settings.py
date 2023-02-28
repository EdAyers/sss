import logging
from pathlib import Path
from typing import Optional
from pydantic import SecretStr, BaseModel, Field, PostgresDsn, BaseSettings
from datetime import timedelta
from miniscutil import Current, get_app_cache_dir, get_app_config_dir, get_workspace_dir
from miniscutil.config import persist_config, get_config

APP_NAME = "blobular"  # [todo] get from my package name

logger = logging.getLogger(APP_NAME)


class Settings(BaseSettings, Current):
    """This dataclass contains all of the configuration needed to use hitsave."""

    cloud_url: str = Field(
        default="https://blobular.edayers.com"
    )  # [todo] switch for local mode.
    """ URL for cloud API server. """

    web_url: str = Field(default="hitsave.io")
    """ URL for the website. """

    local_cache_dir: Path = Field(default_factory=lambda: get_app_cache_dir(APP_NAME))
    """ This is the directory where hitsave should store local caches of data. """

    api_key: Optional[SecretStr] = Field(default=None)

    jwt: Optional[SecretStr] = Field(default=None)

    workspace_dir: Optional[Path] = Field(default_factory=lambda: get_workspace_dir())
    """ Directory for the current project, should be the same as workspace_folder in vscode.
    It defaults to the nearest parent folder containing pyproject.toml or git root. """

    config_dir: Path = Field(default_factory=lambda: get_app_config_dir(APP_NAME))
    """ The root config directory. """

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def secrets_file(self) -> Path:
        return self.config_dir / "secrets.json"

    def get_jwt(self) -> Optional[str]:
        if self.jwt is None:
            try:
                jwt = get_config(self.secrets_file, ("jwt", self.cloud_url))
            except LookupError:
                logger.debug(
                    f"no JWT for {self.cloud_url} found in {self.secrets_file}"
                )
                return None
            self.jwt = SecretStr(jwt)
            return jwt
        else:
            return self.jwt.get_secret_value()

    def persist_jwt(self, jwt: str):
        logger.debug(f"Saving JWT to {self.secrets_file}")
        persist_config(self.secrets_file, ("jwt", self.cloud_url), jwt)
        self.jwt = SecretStr(jwt)

    def invalidate_jwt(self):
        persist_config(self.secrets_file, ("jwt", self.cloud_url), None)
        self.jwt = None

    def get_api_key(self) -> Optional[str]:
        if self.api_key is None:
            try:
                key = get_config(self.secrets_file, ("api_key", self.cloud_url))
                assert isinstance(key, str)
            except LookupError:
                logger.debug(
                    f"no API key for {self.cloud_url} found in {self.secrets_file}"
                )
                return None
            self.api_key = SecretStr(key)
            return key
        else:
            return self.api_key.get_secret_value()

    def persist_api_key(self, key: str):
        logger.debug(f"Saving API key to {self.secrets_file}")
        persist_config(self.secrets_file, ("api_key", self.cloud_url), key)
        self.api_key = SecretStr(key)
