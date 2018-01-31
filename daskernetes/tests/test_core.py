import getpass
import os
from time import sleep, time
import yaml
import tempfile

import pytest
from daskernetes import KubeCluster
from daskernetes.objects import make_pod_spec
from dask.distributed import Client
from distributed.utils_test import loop, inc


def test_basic(image_name, loop):
    with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster:
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


def test_logs(image_name, loop):
    with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster:
        cluster.scale_up(2)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)
            assert time() < start + 20

        a, b = cluster.pods()
        logs = cluster.logs(a)
        assert 'distributed.worker' in logs


def test_ipython_display(image_name, loop):
    ipywidgets = pytest.importorskip('ipywidgets')
    with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster:
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


def test_namespace(image_name, loop):
    with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster:
        assert 'dask' in cluster.name
        assert getpass.getuser() in cluster.name
        with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster2:
            assert cluster.name != cluster2.name


def test_adapt(image_name, loop):
    with KubeCluster(make_pod_spec(image_name), loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(inc, 10)
            result = future.result()
            assert result == 11

        start = time()
        while cluster.scheduler.workers:
            sleep(0.1)
            assert time() < start + 10


def test_env(image_name, loop):
    with KubeCluster(make_pod_spec(image_name, env={'ABC': 'DEF'}), loop=loop) as cluster:
        cluster.scale_up(1)
        with Client(cluster) as client:
            while not cluster.scheduler.workers:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v['ABC'] == 'DEF' for v in env.values())

def test_pod_from_yaml(image_name, loop):
    test_yaml = {
        "kind": "Pod",
        "metadata": {
            "labels": {
            "app": "dask",
            "component": "dask-worker"
            }
        },
        "spec": {
            "containers": [
            {
                "args": [
                    "dask-worker",
                    "$(DASK_SCHEDULER_ADDRESS)",
                    "--nthreads",
                    "1"
                ],
                "image": image_name,
                "name": "dask-worker"
            }
            ]
        }
    }

    with tempfile.NamedTemporaryFile('w') as f:
        yaml.safe_dump(test_yaml, f)
        f.flush()
        cluster = KubeCluster.from_yaml(
            f.name,
            loop=loop,
            n_workers=0,
        )

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
