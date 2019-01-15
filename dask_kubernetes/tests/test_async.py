import uuid

import kubernetes_asyncio as kubernetes
import pytest

from dask_kubernetes import KubeCluster, ClusterAuth, make_pod_spec


@pytest.fixture
def pod_spec(image_name):
    yield make_pod_spec(
        image=image_name,
        extra_container_config={'imagePullPolicy': 'IfNotPresent'}
    )


@pytest.fixture
async def api_async():
    await ClusterAuth.load_first()
    return kubernetes.client.CoreV1Api()


@pytest.fixture
async def ns_async(api_async):
    name = 'test-dask-kubernetes' + str(uuid.uuid4())[:10]
    ns = kubernetes.client.V1Namespace(metadata=kubernetes.client.V1ObjectMeta(name=name))
    await api_async.create_namespace(ns)
    try:
        yield name
    finally:
        await api_async.delete_namespace(name, kubernetes.client.V1DeleteOptions())


@pytest.mark.asyncio
async def test_cluster_create(pod_spec, ns_async):
    async with KubeCluster(pod_spec, namespace=ns_async, asynchronous=True) as cluster:
        pass
