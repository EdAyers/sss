from miniscutil import human_size


def test_human_size(snapshot):
    sizes = [
        1,
        100,
        999,
        1000,
        1024,
        2000,
        2048,
        3000,
        9999,
        10000,
        2048000000,
        9990000000,
        9000000000000000000000,
    ]
    snapshot.assert_match("\n".join(map(human_size, sizes)) + "\n", "bytes")
