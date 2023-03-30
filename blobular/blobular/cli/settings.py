import logging
from pathlib import Path
from typing import Optional
from pydantic import SecretStr, BaseModel, Field, PostgresDsn, BaseSettings
from datetime import timedelta
from miniscutil import Current, get_app_cache_dir, get_app_config_dir, get_workspace_dir
from miniscutil.config import persist_config, get_config, SecretPersist

APP_NAME = "blobular"  # [todo] get from my package name

logger = logging.getLogger(APP_NAME)


class Settings(BaseSettings, Current, SecretPersist):
    """This dataclass contains all of the configuration needed to use hitsave."""

    cloud_url: str = Field(default="https://blobular.edayers.com")

    web_url: str = Field(default="blobular.edayers.com")
    """ URL for the website. """

    local_cache_dir: Path = Field(default_factory=lambda: get_app_cache_dir(APP_NAME))
    """ This is the directory where hitsave should store local caches of data. """

    api_key: Optional[SecretStr] = Field(default=None, is_secret=True)

    jwt: Optional[SecretStr] = Field(default=None, is_secret=True)

    workspace_dir: Optional[Path] = Field(default_factory=lambda: get_workspace_dir())
    """ Directory for the current project, should be the same as workspace_folder in vscode.
    It defaults to the nearest parent folder containing pyproject.toml or git root. """

    config_dir: Path = Field(default_factory=lambda: get_app_config_dir(APP_NAME))
    """ The root config directory. """

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        secret_postfix = lambda self: self.cloud_url

    @property
    def secrets_file(self) -> Path:
        return self.config_dir / "secrets.json"

    def get_jwt(self) -> Optional[str]:
        return self.get_secret("jwt")

    def persist_jwt(self, jwt: str):
        self.persist_secret("jwt", jwt)

    def invalidate_jwt(self):
        self.invalidate_secret("jwt")

    def invalidate_api_key(self):
        self.invalidate_secret("api_key")

    def get_api_key(self) -> Optional[str]:
        return self.get_secret("api_key")

    def persist_api_key(self, key: str):
        self.persist_secret("api_key", key)
