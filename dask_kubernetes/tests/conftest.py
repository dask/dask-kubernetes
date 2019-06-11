import pytest


def pytest_addoption(parser):
    parser.addoption("--worker-image", help="Worker image to use for testing")


@pytest.fixture
def image_name(request):
    worker_image = request.config.getoption("--worker-image")
    if not worker_image:
        pytest.fail(
            "Need to pass --worker-image. "
            "Image must have the same Python and dask versions as host"
        )
    return worker_image
