import pytest

from dask_kubernetes.auth import KubeConfig

def pytest_addoption(parser):
    parser.addoption(
        "--worker-image",
        default="daskdev/dask:dev",help="Worker image to use for testing"
    )
    parser.addoption("--context", default=None, help="kubectl context to use")


@pytest.fixture
def image_name(request):
    return request.config.getoption("--worker-image")


@pytest.fixture(scope="session")
def context(request):
    return request.config.getoption("--context")


@pytest.fixture(scope="session")
def auth(context):
    return [KubeConfig(context=context)] if context else None
