import os
from time import sleep, time

import pytest
from daskernetes import KubeCluster
from dask.distributed import Client
from distributed.utils_test import loop, inc


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


def test_namespace(loop):
    with KubeCluster(loop=loop) as cluster:
        assert 'dask' in cluster.name
        assert os.environ['USER'] in cluster.name
        with KubeCluster(loop=loop, port=0) as cluster2:
            assert cluster.name != cluster2.name
