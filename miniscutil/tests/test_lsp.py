import re
from miniscutil.lsp import LspServer
from miniscutil.lsp.document import DocumentContext, Position, setdoc
from miniscutil.misc import set_ctx
from miniscutil.rpc import Transport
from hypothesis import assume, given, strategies as st
import pytest


class MockTransport(Transport):
    async def recv(self):
        return b"hello world"

    async def send(self, msg):
        pass


@pytest.fixture
def lsp_server():
    yield LspServer(transport=MockTransport())


SURROGATE_KEY = re.compile("[\ud800-\udbff]", re.UNICODE)


def encoding_prop(x: str):
    assert SURROGATE_KEY.match(x) is None, "no surrogate pairs allowed"

    with setdoc(x):
        for offset in range(len(x) + 1):
            position = Position.of_offset(offset)
            offset_2 = position.to_offset()
            assert offset == offset_2


@given(st.text())
def test_encoding(x: str):
    encoding_prop(x)


SURROGATE_KEY = re.compile("[\ud800-\udbff]", re.UNICODE)


# [todo] fails for '\ud800'
@given(st.characters())
def test_encoding_chars(x: str):
    assume(SURROGATE_KEY.match(x) is None)
    encoding_prop(x)


def test_encoding1():
    encoding_prop("")
    x = "a𐐀b"
    encoding_prop(x)
    with setdoc(x):
        for offset, c in enumerate([0, 1, 3, 4]):
            assert Position.of_offset(offset).character == c
    x = "a𐐀b\n\na𐐀b"
    encoding_prop(x)
    with setdoc(x) as doc:
        assert doc.get_line(0) == "a𐐀b\n"
        assert doc.get_line(1) == "\n"
        assert doc.get_line(2) == "a𐐀b"

        for offset, (l, c) in enumerate(
            [(0, 0), (0, 1), (0, 3), (0, 4), (1, 0), (2, 0), (2, 1), (2, 3), (2, 4)]
        ):
            pos = Position.of_offset(offset)
            assert pos.line == l
            assert pos.character == c


def test_encoding2():
    s = "🍓"
    encoding_prop(s)


def test_encoding3():
    encoding_prop("𝓅")


def test_encoding4():
    x = "\ud801"
    with setdoc(x):
        offset = 1
        position = Position.of_offset(offset)
        offset_2 = position.to_offset()
        assert offset_2 == 0


if __name__ == "__main__":
    test_encoding4()
