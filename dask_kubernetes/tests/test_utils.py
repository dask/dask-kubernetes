from dask_kubernetes.utils import b64_dump, b64_load


def test_b64():
    for obj in [
        "hello",
        1,
        True,
        {"Hello": "world"},
        [1, 2, 3, 4],
    ]:
        assert b64_load(b64_dump(obj)) == obj