from time import sleep, time

import pytest
from daskernetes import KubeCluster
from dask.distributed import Client
from distributed.utils_test import loop, inc
from kubernetes import client, config


try:
    config.load_incluster_config()
except config.ConfigException:
    config.load_kube_config()
api = client.CoreV1Api()


def test_basic(loop):
    with KubeCluster(loop=loop) as cluster:
        cluster.scale_up(2)
        with Client(cluster) as client:
            future = client.submit(inc, 10)
            result = future.result()
            assert result == 11

            while len(cluster.scheduler.workers) < 2:
                sleep(0.1)

            # Ensure that inter-worker communication works well
            futures = client.map(inc, range(10))
            total = client.submit(sum, futures)
            assert total.result() == sum(map(inc, range(10)))
            assert all(client.has_what().values())


def test_logs(loop):
    with KubeCluster(loop=loop) as cluster:
        cluster.scale_up(2)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)
            assert time() < start + 20

        a, b = cluster.pods()
        logs = cluster.logs(a)
        assert 'distributed.worker' in logs


def test_ipython_display(loop):
    ipywidgets = pytest.importorskip('ipywidgets')
    with KubeCluster(loop=loop) as cluster:
        cluster.scale_up(1)
        cluster._ipython_display_()
        box = cluster._cached_widget
        assert isinstance(box, ipywidgets.Widget)
        cluster._ipython_display_()
        assert cluster._cached_widget is box

        start = time()
        workers = [child for b in box.children for child in b.children
                   if child.description == 'Actual'][0]
        while workers.value == 0:
            assert time() < start + 10
            sleep(0.5)


def ns_exists(ns, api=api):
    return any(n.metadata.name == ns for n in api.list_namespace().items)


def test_namespace(loop):
    assert not ns_exists('foo')
    with KubeCluster(loop=loop, namespace='foo') as cluster:
        assert cluster.namespace == 'foo'
        assert ns_exists('foo', api=cluster.api)
        with KubeCluster(loop=loop, namespace='foo', port=0) as cluster_2:
            assert cluster_2.namespace == 'foo'
            assert ns_exists('foo', api=cluster.api)

        assert ns_exists('foo', api=cluster.api)

    start = time()
    while ns_exists('foo'):
        sleep(0.1)
        assert time() < start + 10


def test_namespace_random(loop):
    with KubeCluster(loop=loop, namespace=True) as cluster:
        assert cluster.namespace
        ns = cluster.namespace
        assert ns_exists(ns)
        assert not cluster.namespace.isalpha()

    # assert not ns_exists(ns)
    start = time()
    while ns_exists(ns):
        sleep(0.1)
        assert time() < start + 10


def test_namespace_raises(loop):
    assert not ns_exists('foo-123')
    with pytest.raises(ValueError):
        with KubeCluster(loop=loop, namespace='foo-123',
                         create_namespace=False, port=0):
            pass
