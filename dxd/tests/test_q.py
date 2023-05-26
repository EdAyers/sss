from dxd.q import QuotedString, parse_sql, sql, parse_commands
import pytest

examples = [
    ("SELECT * FROM foo WHERE x = 3 + 2", (), {}),
    ("SELECT COUNT(*) FROM foo", (), {}),
]

# test that the sql parse can parse the examples

@pytest.mark.parametrize("example", examples)
def test_parse_sql(example):
  q, args, kwargs = example
  cmds = parse_commands(q, *args, **kwargs)
  qs = [sql(cmd) for cmd in cmds]
  print(qs)

if __name__ == "__main__":
    for q, args, kwargs in examples:
        cmds = parse_commands(q, *args, **kwargs)
        qs = [sql(cmd) for cmd in cmds]
        print(qs)
