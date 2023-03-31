from dataclasses import dataclass, fields
from pathlib import Path
from miniscutil import get_git_root


def test_get_root_is_repo():
    x = get_git_root()
    assert isinstance(x, Path)
    assert x is not None


def test_get_root_no_output(tmp_path, capfd):
    # https://stackoverflow.com/questions/20507601/writing-a-pytest-function-for-checking-the-output-on-console-stdout
    r = get_git_root(tmp_path)
    assert r is None
    out, err = capfd.readouterr()
    assert out == ""
    assert err == ""
