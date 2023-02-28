from pathlib import Path
from typing import Literal, Optional
from pydantic import SecretStr, BaseModel, Field, PostgresDsn, BaseSettings
from datetime import timedelta
from miniscutil import Current


class Settings(BaseSettings, Current):
    """Configuration for API.

    reference: https://docs.pydantic.dev/usage/settings/
    use secret files!: https://docs.pydantic.dev/usage/settings/#secret-support
    """

    github_client_id: str
    github_client_secret: SecretStr
    github_user_agent: str

    jwt_expires: timedelta = Field(default=timedelta(days=30))
    """ Default expiration time for JWT tokens that we issue. """
    jwt_algorithm: str = Field(default="HS256")
    jwt_secret: SecretStr

    blobstore_mode: Literal['localfile', 's3'] = Field(default='localfile')
    database_mode: Literal["sqlite", "postgres"] = Field(default="sqlite")
    pg: Optional[PostgresDsn] = Field(default=None)
    local_data_path: Path = Field(default=Path('data'))

    aws_access_key_id: str
    aws_secret_access_key: SecretStr

    cloud_url: str
    """ The URL of the API. """

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
