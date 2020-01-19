def pytest_addoption(parser):
    parser.addoption(
        "--worker-image",
        default="daskdev/dask:latest",
        help="Worker image to use for testing",
    )
    parser.addoption("--context", default=None, help="kubectl context to use")
    parser.addoption(
        "--in-cluster", action="store_true", default=False, help="are we in cluster?"
    )
    parser.addoption("--namespace", default="default", help="Cluster namespace to use")
