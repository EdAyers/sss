from dataclasses import dataclass, fields
from miniscutil import Current
from pytest import raises


def test_current():
    @dataclass
    class Hello(Current):
        x: int

        @classmethod
        def default(cls):
            return cls(x=100)

    assert "_tokens" not in [f.name for f in fields(Hello)]
    assert "CURRENT" in vars(Hello)
    assert Hello.current().x == 100

    with Hello(5):
        assert Hello.current().x == 5

    assert Hello.current().x == 100

    h = Hello(10)

    assert "CURRENT" not in vars(h)

    with h:
        assert Hello.current().x == 10
        with h:
            assert Hello.current().x == 10
        assert Hello.current().x == 10

    assert Hello.current().x == 100

    try:
        with h:
            raise Exception("oops")
    except Exception:
        pass

    assert Hello.current().x == 100

    @dataclass
    class World(Current):
        y: str

    assert World.CURRENT != Hello.CURRENT

    with raises(TypeError):
        World.current()
