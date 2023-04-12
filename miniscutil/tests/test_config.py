from pathlib import Path
from typing import Optional
from pydantic import BaseSettings, Field, SecretStr
from miniscutil import Current, SecretPersist


def test_persist(tmp_path: Path):
    class TestSettings(BaseSettings, Current, SecretPersist):
        asdf: Optional[SecretStr] = Field(default=None, is_secret=True)

        @property
        def secrets_file(self):
            return tmp_path / "secrets.json"

    s = TestSettings.current()
    assert s.asdf is None
    x = s.get_secret("asdf")
    assert x is None
    expect = "hello world"
    s.persist_secret("asdf", expect)
    assert s.get_secret("asdf") == expect
