import getpass
from time import time
import uuid

import kubernetes_asyncio as kubernetes
import pytest
from tornado import gen

import dask
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


@pytest.fixture
async def cluster(pod_spec, ns):
    async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
        yield cluster


@pytest.fixture
async def client(cluster):
    async with Client(cluster, asynchronous=True) as client:
        yield client


@pytest.mark.asyncio
async def test_cluster_create(pod_spec, ns):
    async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
        cluster.scale(1)
        async with Client(cluster, asynchronous=True) as client:
            result = await client.submit(lambda x: x + 1, 10)
            assert result == 11


@pytest.mark.asyncio
async def test_basic(cluster, client):
    cluster.scale(2)
    future = client.submit(lambda x: x + 1, 10)
    result = await future
    assert result == 11

    while len(cluster.scheduler.workers) < 2:
        await gen.sleep(0.1)

    # Ensure that inter-worker communication works well
    futures = client.map(lambda x: x + 1, range(10))
    total = client.submit(sum, futures)
    assert (await total) == sum(map(lambda x: x + 1, range(10)))
    assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_logs(cluster):
    cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) < 2:
        await gen.sleep(0.1)
        assert time() < start + 20

    a, b = await cluster.pods()
    logs = await cluster.logs(a)
    assert 'distributed.worker' in logs

    logs = await cluster.logs()
    assert len(logs) == 2
    for pod in logs:
        assert 'distributed.worker' in logs[pod]


@pytest.mark.asyncio
async def test_dask_worker_name_env_variable(pod_spec, ns):
    with dask.config.set({'kubernetes.name': 'foo-{USER}-{uuid}'}):
        async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
            assert 'foo-' + getpass.getuser() in cluster.name


@pytest.mark.asyncio
async def test_diagnostics_link_env_variable(pod_spec, ns):
    pytest.importorskip('bokeh')
    with dask.config.set({'distributed.dashboard.link': 'foo-{USER}-{port}'}):
        async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
            port = cluster.scheduler.services['bokeh'].port
            cluster._ipython_display_()
            box = cluster._cached_widget

            assert 'foo-' + getpass.getuser() + '-' + str(port) in str(box)


@pytest.mark.asyncio
async def test_namespace(pod_spec, ns):
    async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
        assert 'dask' in cluster.name
        assert getpass.getuser() in cluster.name
        async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster2:
            assert cluster.name != cluster2.name

            cluster2.scale(1)
            while len(await cluster2.pods()) != 1:
                await gen.sleep(0.1)


@pytest.mark.asyncio
async def test_adapt(cluster):
    cluster.adapt()
    async with Client(cluster, asynchronous=True) as client:
        future = client.submit(lambda x: x + 1, 10)
        result = await future
        assert result == 11

    start = time()
    while cluster.scheduler.workers:
        await gen.sleep(0.1)
        assert time() < start + 10
