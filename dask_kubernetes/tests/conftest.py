import pytest


def pytest_addoption(parser):
    parser.addoption("--worker-image", help="Worker image to use for testing")


@pytest.fixture
def image_name(request):
    worker_image = request.config.getoption("--worker-image")
    if not worker_image:
        return "daskdev/dask:dev"
    return worker_image
