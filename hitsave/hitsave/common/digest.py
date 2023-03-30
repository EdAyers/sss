from typing import NewType
from blake3 import blake3

Digest = NewType("Digest", str)


def digest_string(x: str) -> Digest:
    """String to blake3 hex-digest"""
    h = blake3()  # type: ignore
    h.update(x.encode())
    s: str = h.hexdigest()
    return Digest(s)


def digest_dictionary(d: dict[str, str]) -> Digest:
    h = blake3()  # type: ignore
    h.update(b"{")
    for k in sorted(d.keys()):
        h.update(k.encode())
        h.update(b":")
        v = d[k]
        if isinstance(v, str):
            v = v.encode()
        assert isinstance(v, bytes)
        h.update(v)
        h.update(b",")
    h.update(b"}")
    return Digest(h.hexdigest())
