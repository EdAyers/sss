import shlex
import re
import typing
from typing import Type, ClassVar, TypeVar, Optional
import types
from contextlib import contextmanager

""" Utilities for parsing sql queries. """


def tokenize(text: str):
    return list(shlex.shlex(text, punctuation_chars="."))


def is_word(text: str):
    return re.match(r"[\w_][\w\-_\d]*", text) is not None


assert is_word("foo")

T = TypeVar("T")


class ParseError(RuntimeError):
    pass


class ParseState:
    type_registry: ClassVar[dict[str, Type]] = {}
    case_insensitive_literals = True

    def __str__(self):
        ts = [f"⟨{t}⟩" for t in self.tokens]
        ts.insert(self.pos, "•")
        return " ".join(ts)

    def __init__(self, s: str):
        self.tokens = tokenize(s)
        self.pos = 0

    def is_eof(self):
        return self.pos >= len(self.tokens)

    def take_word(self):
        item = self.next()
        assert is_word(item)
        return item

    def next(self):
        item = self.tokens[self.pos]
        self.pos += 1
        return item

    def take_int_literal(self):
        item = self.next()
        return int(item)

    def take_one(self, xs: typing.Sequence[str], case_sensitive=True):
        for x in xs:
            try:
                with self.attempt():
                    self.take(x, case_sensitive=case_sensitive)
                    return x
            except:
                pass
        raise ValueError(f"Expected one of {xs}")

    def try_take(self, x: str) -> bool:
        try:
            with self.attempt():
                self.take(x)
                return True
        except:
            return False

    def try_parse(self, t: Type[T]) -> typing.Optional[T]:
        try:
            with self.attempt():
                return self.parse(t)
        except:
            return None

    def take(self, x: str, case_sensitive=True):
        y = x.lower() if not case_sensitive else x
        item = self.next()
        if not case_sensitive:
            item = item.lower()
        if item == y:
            return x
        y, *rest = tokenize(y)
        if y == item:
            for r in rest:
                item = self.next()
                if not case_sensitive:
                    item = item.lower()
                assert r == item
            return x
        raise ValueError(f"Expected {x}, got {item}")

    def can_take(self, x: str) -> bool:
        try:
            with self.attempt():
                self.take(x)
                return True
        except:
            return False

    @contextmanager
    def attempt(self):
        old_pos = self.pos
        try:
            yield
        except:
            self.pos = old_pos
            raise
        else:
            pass

    def parse(self, t: Type[T]) -> T:
        if isinstance(t, str):
            t = self.type_registry[t]
        if hasattr(t, "__parse__"):
            # [note] t.__parse__(self) won't keep get_args() information.
            return t.__parse__.__func__(t, self)  # type: ignore
        if t is str:
            return self.take_word()  # type: ignore
        if t is None or t is type(None):
            return None  # type: ignore
        Orig = typing.get_origin(t)
        args = typing.get_args(t)
        if Orig is typing.Literal:
            return self.take_one(args, case_sensitive=not self.case_insensitive_literals)  # type: ignore
        if Orig is typing.Union or Orig is types.UnionType:
            for arg in args:
                # [todo] we can be more fancy than this with coroutines.
                if arg is None or arg is type(None):
                    # none case always has lowest parsing priority.
                    continue
                try:
                    with self.attempt():
                        return self.parse(arg)
                except:
                    pass
            if None in args or type(None) in args:
                return None  # type: ignore
            else:
                # [todo] can do cool stuff with 3.11 exception groups here.
                raise ValueError(f"Expected one of {args}")
        if Orig is list:
            X = args[0]
            # we always assume these kinds of lists are not empty
            xs = []
            xs.append(self.parse(X))
            while True:
                try:
                    with self.attempt():
                        self.take(",")
                        xs.append(self.parse(X))
                except:
                    break
            return xs  # type: ignore
        if Orig is tuple:
            xs = []
            for X in args:
                xs.append(self.parse(X))
            return tuple(xs)  # type: ignore

        raise NotImplementedError(f"Cannot parse {t}")


def run_parser(t: Type[T], s: str, non_eof_ok=False) -> T:
    p = ParseState(s)
    item = p.parse(t)
    return item
