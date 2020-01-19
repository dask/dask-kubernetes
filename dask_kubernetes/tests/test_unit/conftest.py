import pytest


@pytest.fixture
def no_kubernetes(mocker):
    """Patch all external comms to Kubernetes."""
    from kubernetes.client.api_client import ApiClient
    from kubernetes_asyncio.client.api_client import ApiClient as ApiClient_async

    mocker.patch.object(ApiClient, "__new__")
    mocker.patch.object(ApiClient_async, "__new__")

    # FixMe: this is only needed due to call-time import of non-async kubernetes
    mocker.patch("dask_kubernetes.core._cleanup_resources")
