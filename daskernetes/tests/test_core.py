import getpass
import os
from time import sleep, time
import yaml

import pytest
import daskernetes
from daskernetes import KubeCluster
from daskernetes.core import deserialize
from dask.distributed import Client
from distributed.utils import tmpfile
from distributed.utils_test import loop, inc

from kubernetes import client


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
        assert getpass.getuser() in cluster.name
        with KubeCluster(loop=loop, port=0) as cluster2:
            assert cluster.name != cluster2.name


def test_adapt(loop):
    with KubeCluster(loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(inc, 10)
            result = future.result()
            assert result == 11

        start = time()
        while cluster.scheduler.workers:
            sleep(0.1)
            assert time() < start + 10


def test_env(loop):
    with KubeCluster(loop=loop, env={'ABC': 'DEF'}) as cluster:
        cluster.scale_up(1)
        with Client(cluster) as client:
            while not cluster.scheduler.workers:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v['ABC'] == 'DEF' for v in env.values())


def test_deserialize():
    pod = client.V1Pod(
            metadata=client.V1ObjectMeta(name='foo', labels={}),
            spec=client.V1PodSpec(containers=[
                client.V1Container(
                    name='dask-worker',
                    image='foo:latest')
                ])
    )
    assert deserialize(pod, client.V1Pod) is pod

    d = pod.to_dict()

    pod2 = deserialize(d, client.V1Pod)
    assert type(pod2) == type(pod)
    assert pod2.to_dict() == pod.to_dict()

    with tmpfile() as fn:
        with open(fn, 'w') as f:
            yaml.dump(d, f)

        pod3 = deserialize(fn, client.V1Pod)
        assert type(pod3) == type(pod)
        assert pod3.to_dict() == pod.to_dict()


def test_config(loop):
    spec = client.V1PodSpec(containers=[
                client.V1Container(
                    name='dask-worker',
                    image='foo:latest')
                ])
    if 'worker' not in daskernetes.config:
        daskernetes.config['worker'] = {}
    daskernetes.config['worker']['spec'] = spec.to_dict()

    with KubeCluster(loop=loop) as cluster:
        assert cluster.worker_spec.containers[0].image == 'foo:latest'
