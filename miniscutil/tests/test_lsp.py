from miniscutil.lsp import LspServer
from miniscutil.lsp.document import DocumentContext, Position, setdoc
from miniscutil.misc import set_ctx
from miniscutil.rpc import Transport
from hypothesis import given, strategies as st
import pytest


class MockTransport(Transport):
    async def recv(self):
        return b"hello world"

    async def send(self, msg):
        pass


@pytest.fixture
def lsp_server():
    yield LspServer(transport=MockTransport())


def encoding_prop(x: str):
    with setdoc(x):
        for offset in range(len(x) + 1):
            position = Position.of_offset(offset)
            offset_2 = position.to_offset()
            assert offset == offset_2


@given(st.text())
def test_encoding(x: str):
    encoding_prop(x)


@given(st.characters())
def test_encoding_chars(x: str):
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


if __name__ == "__main__":
    test_encoding1()
