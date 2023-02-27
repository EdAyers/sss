from pathlib import Path
import random
from miniscutil.misc import chunked_read

import pytest
from blobular.cli.state import AppState


@pytest.fixture()
def appstate(tmp_path: Path):
    yield AppState.of_dir(tmp_path / "state")


def test_e2e(tmp_path: Path, appstate: AppState):
    infile = tmp_path / "input.bin"
    outfile = tmp_path / "output.bin"
    with infile.open("wb") as f:
        for _ in range(1024):
            f.write(random.randbytes(1024))
    with infile.open("rb") as f:
        info = appstate.store.add(f)

    appstate.store.flush()

    with appstate.store.open(info.digest) as f:
        with outfile.open("wb") as g:
            for bs in chunked_read(f):
                g.write(bs)

    for a, b in zip(chunked_read(infile.open("rb")), chunked_read(outfile.open("rb"))):
        assert a == b
