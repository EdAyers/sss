from uxu.listdiff import diff, Reorder
from hypothesis import given, assume
import hypothesis.strategies as hs
from itertools import product


def texts():
    return hs.text("abcdefg")


@hs.composite
def expanders(draw):
    l1 = list(draw(texts()))
    l2 = list(draw(texts()))
    indices = draw(hs.sets(hs.integers(min_value=0, max_value=len(l1))))
    expander = {i: draw(hs.integers(min_value=0, max_value=10)) for i in indices}
    return l1, l2, expander


def flatten(l):
    def core(x):
        if isinstance(x, list):
            for y in x:
                yield from core(y)
        else:
            yield x

    return list(core(l))


def expander_prop(xs):
    l1, l2, expander = xs
    δ = diff(l1, l2)
    δe = δ.expand_elements(expander)
    l2i = δ.apply(list(range(len(l1))))
    assert len(l2) == len(l2i)

    l1e = flatten(
        [
            [f"{x}-{k}" for k in range(expander[i])] if i in expander else x
            for i, x in enumerate(l1)
        ]
    )
    l2e = flatten(
        [
            [f"{l2[j]}-{k}" for k in range(expander[x])]
            if (isinstance(x, int) and x in expander)
            else l2[j]
            for j, x in enumerate(l2i)
        ]
    )

    l2ex = δe.apply(l1e)

    assert len(l2ex) == len(l2e)
    assert all(x == y for x, y in zip(l2ex, l2e))


def validate_prop(l1, l2):
    r = diff(l1, l2)
    r.validate()


def recover_prop(l1, l2):
    r = diff(l1, l2)
    l3 = r.apply(l1)
    assert len(l2) == len(l3)
    assert all(x is y for x, y in zip(l2, l3))


def invert_prop(l1, l2):
    r = diff(l1, l2).compose(diff(l2, l1)).apply(l1)
    assert len(l1) == len(r)
    assert all(x is y for x, y in zip(l1, r))


def compose_prop(l1, l2, l3):
    δ12 = diff(l1, l2)
    δ23 = diff(l2, l3)

    δ13x = δ12.compose(δ23)
    δ13x.validate()

    l3x = δ13x.apply(l1)
    assert len(l3) == len(l3x)
    assert all(x is y for x, y in zip(l3, l3x))


def concat_prop(l1, l2, l3, l4):
    δ1_2 = diff(l1, l2)
    δ3_4 = diff(l3, l4)
    δ13_24 = δ1_2.concat(δ3_4)
    δ13_24.validate()
    r1 = δ13_24.apply(l1 + l3)
    r2 = l2 + l4
    assert len(r1) == len(r2)
    assert all(x is y for x, y in zip(r1, r2))


examples = ["", "a", "aa", "aaa", "ab", "ba", "baa", "bba", "aab"]
examples = [list(x) for x in examples]


def test_recover_explicit():
    for x, y in product(examples, examples):
        recover_prop(x, y)


def test_validate_explicit():
    for x, y in product(examples, examples):
        validate_prop(x, y)


def test_prop_compose():
    for x, y, z in product(examples, examples, examples):
        compose_prop(x, y, z)


def test_prop_concat():
    for x, y, z, w in product(examples, examples, examples, examples):
        concat_prop(x, y, z, w)


def test_expander1():
    x = (["a"], ["a"], {0: 2})
    expander_prop(x)


@given(texts(), texts())
def test_recover_chars(x1: str, x2: str):
    recover_prop(list(x1), list(x2))


@given(texts(), texts(), texts())
def test_compose_chars(x1: str, x2: str, x3: str):
    compose_prop(list(x1), list(x2), list(x3))


@given(texts(), texts(), texts(), texts())
def test_concat_chars(x1: str, x2: str, x3: str, x4: str):
    concat_prop(list(x1), list(x2), list(x3), list(x4))


@given(texts(), texts())
def test_validate_chars(x1: str, x2: str):
    validate_prop(list(x1), list(x2))


@given(expanders())
def test_expander_chars(x):
    expander_prop(x)


if __name__ == "__main__":
    for x in examples:
        for y in examples:
            r = diff(x, y)
            r.apply(x)
