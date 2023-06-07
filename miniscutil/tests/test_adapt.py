from typing import Optional
from miniscutil.adapt import adapt, restore


def test_adapt1():
    x = restore(Optional[str], None)
    assert x is None
    x = restore(Optional[str], "hello")
    assert x == "hello"
