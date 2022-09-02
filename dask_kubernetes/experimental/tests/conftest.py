import pytest

from dask_kubernetes.experimental import KubeCluster


@pytest.fixture
def cluster(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="foo", image=docker_image) as cluster:
            yield cluster
