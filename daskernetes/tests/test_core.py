from time import sleep

from daskernetes import KubeCluster
from dask.distributed import Client
from distributed.utils_test import loop


def test_basic(loop):
    with KubeCluster(loop=loop) as cluster:
        cluster.scale_up(1)
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11


def test_logs(loop):
    with KubeCluster(loop=loop) as cluster:
        cluster.scale_up(2)
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)

        a, b = cluster.pods()
        logs = cluster.logs(a)
        assert 'distributed.worker' in logs
