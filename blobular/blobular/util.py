from functools import partial
import math
from typing import IO, Iterator


def chunked_read(x: IO[bytes], block_size=2**20) -> Iterator[bytes]:
    """Repeatededly read in BLOCK_SIZE chunks from the BufferedReader until it's empty."""
    # iter(f, x) will call f repeatedly until x is returned and then stop
    # https://docs.python.org/3/library/functions.html#iter
    return iter(partial(x.read, block_size), b"")


def human_size(bytes: int, units=[" bytes", "KB", "MB", "GB", "TB", "PB", "EB"]):
    """Returns a human readable string representation of bytes.

    [todo] use humanize library (so you can localise too)
    """
    if bytes == 1:
        return "1 byte"
    if bytes < (2**10):
        return str(bytes) + units[0]
    ll = math.log2(bytes)
    i = int(ll // 10)
    if i >= len(units):
        return "2^" + str(math.ceil(math.log2(bytes))) + " bytes"
    f = bytes / (2 ** (i * 10))
    return f"{f:.1f}{units[i]}"
