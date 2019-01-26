import uuid

import kubernetes_asyncio as kubernetes
import pytest

from dask.distributed import Client
from dask_kubernetes import KubeCluster, ClusterAuth, make_pod_spec


@pytest.fixture
def pod_spec(image_name):
    yield make_pod_spec(
        image=image_name,
        extra_container_config={'imagePullPolicy': 'IfNotPresent'}
    )


@pytest.fixture
async def api():
    await ClusterAuth.load_first()
    return kubernetes.client.CoreV1Api()


@pytest.fixture
async def ns(api):
    name = 'test-dask-kubernetes' + str(uuid.uuid4())[:10]
    ns = kubernetes.client.V1Namespace(metadata=kubernetes.client.V1ObjectMeta(name=name))
    await api.create_namespace(ns)
    try:
        yield name
    finally:
        await api.delete_namespace(name, kubernetes.client.V1DeleteOptions())


@pytest.mark.asyncio
async def test_cluster_create(pod_spec, ns):
    async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
        cluster.scale(1)
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11
