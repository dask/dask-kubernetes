import pytest
import pytest_asyncio

import dask.config

from dask_kubernetes.operator import KubeCluster


@pytest.fixture
def cluster(kopf_runner, docker_image):
    with dask.config.set({"kubernetes.name": "foo-{uuid}"}):
        with kopf_runner:
            with KubeCluster(image=docker_image, n_workers=1) as cluster:
                yield cluster


@pytest_asyncio.fixture
async def async_cluster(kopf_runner, docker_image):
    with dask.config.set({"kubernetes.name": "foo-{uuid}"}):
        with kopf_runner:
            async with KubeCluster(
                image=docker_image, n_workers=1, asynchronous=True
            ) as cluster:
                yield cluster
