import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--worker-image",
        help="Worker image to use for testing"
    )

@pytest.fixture
def image_name(request):
    worker_image = request.config.getoption('--worker-image') or 'daskdev/dask:latest'
    return worker_image
