from miniscutil.type_util import *


def test_isoptional():
    assert is_optional(Optional[int])
    assert not is_optional(int)
    assert is_optional(Union[type(None), int])
    assert is_optional(Union[int, type(None)])
    assert not is_optional(Union[int, int])


def test_asoptional():
    assert as_optional(Optional[int]) is int
    assert as_optional(int) is None
    assert as_optional(Union[type(None), int]) is int
    assert as_optional(Union[int, type(None)]) is int
    assert as_optional(Union[int, int]) is None
    assert as_optional(Optional[Optional[int]]) is int
    assert Union[int, float] == Union[float, int]
    assert as_optional(Optional[type(None)]) is None
    assert as_optional(Union[int, float, type(None)]) == Union[int, float]


def test_aslist():
    assert as_list(list[int]) is int
    assert as_list(int) is None
