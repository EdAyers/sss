from hitsave import experiment


@experiment
def fibo(n):
    print(f"computing fibo({n})!!")
    return 1 if n < 3 else fibo(n - 1) + fibo(n - 2)


def test_fibo():
    assert fibo(20) == fibo(20)


if __name__ == "__main__":
    print(fibo(20))
