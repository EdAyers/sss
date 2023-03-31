from pathlib import Path
from hitsave.local.settings import Settings


def test_config_workspace_dir():
    d = Settings.current().workspace_dir
    assert isinstance(d, Path)
    assert d.exists()


def test_config_local_cache_path():
    d = Settings.current().local_cache_dir
    assert isinstance(d, Path)
    assert d.exists()
