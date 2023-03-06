from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from enum import Enum
from functools import partial
from typing import Any, Callable, Generic, Iterable, Optional, TypeVar, Union
from difflib import SequenceMatcher

from miniscutil import map_keys, Sum

A = TypeVar("A")
B = TypeVar("B")
R = TypeVar("R")


@dataclass
class Reorder(Generic[A]):
    """Represents a patch of removing and inserting items between a pair of `list[A]`s.

    You should not instantiate this yourself, instead use the `diff` function.
    """

    l1_len: int
    l2_len: int
    remove_these: dict[int, Optional[str]]
    then_insert_these: dict[int, Sum[str, A]]

    def apply(self, l1: list[A]) -> list[A]:
        if len(l1) < self.l1_len:
            raise ValueError(f"list length ({len(l1)}) must be at least {self.l1_len}")
        l2 = []
        removed = {}
        for i in range(len(l1)):
            if i in self.remove_these:
                k = self.remove_these[i]
                if k is not None:
                    removed[k] = l1[i]
            else:
                l2.append(l1[i])
        for j in sorted(self.then_insert_these.keys()):
            o = self.then_insert_these[j].match(lambda k: removed[k], lambda t: t)
            l2.insert(j, o)
        return l2

    def map_inserts(self, fn: Callable[[int, A], B]) -> "Reorder[B]":
        x = {j: v.mapr(partial(fn, j)) for j, v in self.then_insert_these.items()}
        return replace(self, then_insert_these=x)  # type: ignore

    def validate(self):
        assert all(isinstance(i, int) for i in self.remove_these.keys())
        assert all(isinstance(j, int) for j in self.then_insert_these.keys())
        assert all(
            j < self.l2_len for j in self.then_insert_these.keys()
        ), "then_insert_these must be in range"
        assert all(
            i < self.l1_len for i in self.remove_these.keys()
        ), "remove_these must be in range"
        rm_keys = [v for v in self.remove_these.values() if v is not None]
        assert len(set(rm_keys)) == len(rm_keys), "remove_these keys must be unique"
        assert all(
            isinstance(v, Sum) for v in self.then_insert_these.values()
        ), "then_insert_these are not Sum types"
        in_keys = [v.value for v in self.then_insert_these.values() if v.is_left]
        assert len(set(in_keys)) == len(
            in_keys
        ), "then_insert_these keys must be unique"
        assert set(in_keys) == set(
            rm_keys
        ), "then_insert_these keys must match remove_these keys"

    def compose(self, after: "Reorder[A]", extend=False) -> "Reorder[A]":
        """Create a `Reorder` that is equivalent to applying `self` followed by `after`."""

        if self.l2_len != after.l1_len:
            n1 = self.l2_len
            n2 = after.l1_len
            if extend:
                if n1 < n2:
                    return self.extend_right(n2 - n1).compose(after, extend=False)
                else:
                    return self.compose(after.extend_right(n1 - n2), extend=False)
            raise ValueError(f"Intermediate lengths do not match")

        δ1: Reorder[Sum[int, A]] = self.map_inserts(lambda j, a: Sum.inr(a))
        δ2: Reorder[Sum[int, A]] = after.map_inserts(lambda k, a: Sum.inr(a))
        l1 = [Sum.inl(i) for i in range(self.l1_len)]
        l2: list[Sum[int, A]] = δ1.apply(l1)
        remove_these = {
            i: (f"1-{v}" if v is not None else None)
            for i, v in self.remove_these.items()
        }
        for j, v in after.remove_these.items():
            s = l2[j]
            if s.is_left:
                i = s.value
                assert isinstance(i, int)
                remove_these[i] = f"2-{v}" if v is not None else None
        l2 = [Sum.inl(j) for j in range(self.l2_len)]
        l3: list[Sum[int, A]] = δ2.apply(l2)
        insert_these = {
            k: v.map(left=lambda v: f"2-{v}", right=lambda a: a)
            for k, v in after.then_insert_these.items()
        }
        for j, v in δ1.then_insert_these.items():
            try:
                k = l3.index(Sum.inl(j))
            except ValueError:
                continue
            if v.is_left:
                s = v.value
                assert isinstance(s, str), f"expected string but got {s} : {type(s)}"
                if k in insert_these:
                    ss = insert_these[k]
                    assert ss.is_left
                else:
                    insert_these[k] = Sum.inl(f"1-{s}")
            else:
                ja: Sum[int, A] = v.value
                assert not ja.is_left
                a: A = ja.value
                insert_these[k] = Sum.inr(a)

        return Reorder(
            l1_len=self.l1_len,
            l2_len=after.l2_len,
            remove_these=remove_these,
            then_insert_these=insert_these,
        )

    def extend_left(self, length: int) -> "Reorder[A]":
        return Reorder(
            length + self.l1_len,
            length + self.l2_len,
            remove_these=map_keys(lambda i: i + length, self.remove_these),
            then_insert_these=map_keys(lambda j: j + length, self.then_insert_these),
        )

    def extend_right(self, length: int) -> "Reorder[A]":
        return Reorder(
            length + self.l1_len,
            length + self.l2_len,
            remove_these=self.remove_these,
            then_insert_these=self.then_insert_these,
        )

    def concat(self, other: "Reorder[A]") -> "Reorder[A]":
        """Horizontally concatenate `self` and `other`."""
        δ1 = self._map_keys(lambda s: f"a-{s}")
        δ2 = other._map_keys(lambda s: f"b-{s}")
        r2 = map_keys(lambda i: δ1.l1_len + i, δ2.remove_these)
        i2 = map_keys(lambda j: δ1.l2_len + j, δ2.then_insert_these)
        r = {**δ1.remove_these, **r2}
        i = {**δ1.then_insert_these, **i2}

        return Reorder(
            self.l1_len + other.l1_len,
            self.l2_len + other.l2_len,
            remove_these=r,
            then_insert_these=i,
        )

    def expand_elements(self, i_to_len: dict[int, int]) -> "Reorder[A]":
        """Return the reordering when the given l1 indices are expanded at the given index."""
        bloat = sum(i_to_len.values())
        nr = 0
        ni = 0
        r = {}
        ins = {}
        for i in range(self.l1_len):
            if i in i_to_len:
                l = i_to_len[i]
                if i in self.remove_these:
                    v = self.remove_these[i]
                    if v is not None:
                        for k in range(l):
                            r[i + nr + k] = f"{k}-{v}"
                    else:
                        for k in range(l):
                            r[i + nr + k] = None
                nr += l - 1
            elif i in self.remove_these:
                r[i + nr] = self.remove_these[i]
        l2 = self.map_inserts(lambda j, a: Sum.inr(a)).apply(list(map(Sum.inl, range(self.l1_len))))  # type: ignore
        for j, ia in enumerate(l2):
            if ia.is_left:
                i: int = ia.value
                if i in i_to_len:
                    l = i_to_len[i]
                    if j in self.then_insert_these:
                        ka = self.then_insert_these[j]
                        assert ka.is_left
                        s: str = ka.value
                        for k in range(l):
                            ins[j + ni + k] = Sum.inl(f"{k}-{s}")
                    ni += l - 1
                else:
                    if j in self.then_insert_these:
                        ins[j + ni] = self.then_insert_these[j]

            else:
                a: A = ia.value
                ins[j + ni] = ia

        return Reorder(
            l1_len=nr,
            l2_len=ni,
            remove_these=r,
            then_insert_these=ins,
        )

    def __add__(self, other: "Reorder[A]") -> "Reorder[A]":
        return self.concat(other)

    def _map_keys(self, fn: Callable[[str], str]) -> "Reorder[A]":
        return Reorder(
            self.l1_len,
            self.l2_len,
            remove_these={
                i: (fn(s) if s is not None else s) for i, s in self.remove_these.items()
            },
            then_insert_these={
                j: v.mapl(fn) for j, v in self.then_insert_these.items()
            },
        )

    @classmethod
    def identity(cls, length: int) -> "Reorder[Any]":
        return cls(l1_len=length, l2_len=length, remove_these={}, then_insert_these={})

    @property
    def is_identity(self):
        return len(self.remove_these) == 0 and len(self.then_insert_these) == 0

    @property
    def deletions(self):
        """Get the indices of items in the first list that are deleted."""
        for i in self.remove_these.keys():
            if self.remove_these[i] is None:
                yield i

    @property
    def creations(self):
        """Get the indices of items in the second list that are created."""
        for j, v in self.then_insert_these.items():
            if not v.is_left:
                yield j

    @property
    def moves(self) -> Iterable[tuple[int, int]]:
        """Get pairs of i,j indices of all pairs of values that appear in both lists."""
        rm = {}
        count = 0
        for i in range(self.l1_len):
            if i in self.remove_these:
                t = self.remove_these[i]
                if t is None:
                    continue
                else:
                    assert t not in rm
                    rm[t] = i
            else:
                assert count not in rm
                rm[count] = i
                count += 1
        rm_count = count
        count = 0
        for j in range(self.l2_len):
            if j in self.then_insert_these:
                v = self.then_insert_these[j]
                if v.is_left:
                    k = v.value
                    i = rm.pop(k)
                    yield (i, j)
                else:
                    continue
            else:
                i = rm.pop(count)
                yield (i, j)
                count += 1
        assert count == rm_count
        assert len(rm) == 0, f"Remaining: {rm}, {self}"


def deduplicate_values(d: dict):
    acc = {}
    c = Counter()
    for k, v in d.items():
        n = c[v]
        acc[k] = f"{v}-{n}"
        c[v] += 1
    return acc


def diff(l1: list[A], l2: list[A]) -> Reorder[A]:
    removes = dict()
    co_removes = dict()
    codes = SequenceMatcher(None, l1, l2).get_opcodes()
    for tag, i1, i2, j1, j2 in codes:
        if tag == "replace" or tag == "delete":
            for i in range(i1, i2):
                removes[i] = str(hash(l1[i]))
        if tag == "replace" or tag == "insert":
            for j in range(j1, j2):
                assert j not in co_removes
                co_removes[j] = str(hash(l2[j]))
    # deduplicate
    removes = deduplicate_values(removes)
    co_removes = deduplicate_values(co_removes)
    # if a new item is inserted we need to include a copy of the item.
    new_inserts = set(co_removes.values()).difference(removes.values())
    deletions = set(removes.values()).difference(co_removes.values())
    remove_these: Any = {i: (None if h in deletions else h) for i, h in removes.items()}
    inserts = {
        i: (Sum.inr(l2[i]) if h in new_inserts else Sum.inl(h))
        for i, h in co_removes.items()
    }

    return Reorder(
        len(l1), len(l2), remove_these=remove_these, then_insert_these=inserts
    )
