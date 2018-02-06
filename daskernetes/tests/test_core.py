import getpass
import os
from time import sleep, time
import yaml

import pytest
from daskernetes import KubeCluster
from daskernetes.objects import make_pod_spec
from dask.distributed import Client
from distributed.utils_test import loop, inc  # noqa: F401
from distributed.utils import tmpfile


@pytest.fixture
def pod_spec(image_name):
    return make_pod_spec(image=image_name)


def test_basic(pod_spec, loop):
    with KubeCluster(pod_spec, loop=loop) as cluster:
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


def test_logs(pod_spec, loop):
    with KubeCluster(pod_spec, loop=loop) as cluster:
        cluster.scale_up(2)

        start = time()
        while len(cluster.scheduler.workers) < 2:
            sleep(0.1)
            assert time() < start + 20

        a, b = cluster.pods()
        logs = cluster.logs(a)
        assert 'distributed.worker' in logs


def test_ipython_display(pod_spec, loop):
    ipywidgets = pytest.importorskip('ipywidgets')
    with KubeCluster(pod_spec, loop=loop) as cluster:
        cluster.scale_up(1)
        cluster._ipython_display_()
        box = cluster._cached_widget
        assert isinstance(box, ipywidgets.Widget)
        cluster._ipython_display_()
        assert cluster._cached_widget is box

        start = time()
        workers = [child for child in box.children
                   if child.description == 'Actual'][0]
        while workers.value == 0:
            assert time() < start + 10
            sleep(0.5)


def test_namespace(pod_spec, loop):
    with KubeCluster(pod_spec, loop=loop) as cluster:
        assert 'dask' in cluster.name
        assert getpass.getuser() in cluster.name
        with KubeCluster(pod_spec, loop=loop) as cluster2:
            assert cluster.name != cluster2.name


def test_adapt(pod_spec, loop):
    with KubeCluster(pod_spec, loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(inc, 10)
            result = future.result()
            assert result == 11

        start = time()
        while cluster.scheduler.workers:
            sleep(0.1)
            assert time() < start + 10


def test_env(pod_spec, loop):
    with KubeCluster(pod_spec, env={'ABC': 'DEF'}, loop=loop) as cluster:
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
            "containers": [{
                "args": [
                    "dask-worker",
                    "$(DASK_SCHEDULER_ADDRESS)",
                    "--nthreads",
                    "1"
                ],
                "image": image_name,
                "name": "dask-worker"
            }]
        }
    }

    with tmpfile(extension='yaml') as fn:
        with open(fn, mode='w') as f:
            yaml.dump(test_yaml, f)
        with KubeCluster.from_yaml(f.name, loop=loop) as cluster:
            cluster.scale_up(2)
            with Client(cluster) as client:
                future = client.submit(inc, 10)
                result = future.result()
                assert result == 11

                start = time()
                while len(cluster.scheduler.workers) < 2:
                    sleep(0.1)
                    assert time() < start + 10, 'timeout'

                # Ensure that inter-worker communication works well
                futures = client.map(inc, range(10))
                total = client.submit(sum, futures)
                assert total.result() == sum(map(inc, range(10)))
                assert all(client.has_what().values())


def test_pod_from_dict(image_name, loop):
    spec = {
        'metadata': {},
        'restartPolicy': 'Never',
        'spec': {
            'containers': [{
                'args': ['dask-worker', '$(DASK_SCHEDULER_ADDRESS)',
                         '--nthreads', '1',
                         '--death-timeout', '60'],
                'command': None,
                'image': image_name,
                'name': 'dask-worker',
            }]
        }
    }

    with KubeCluster.from_dict(spec, loop=loop) as cluster:
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


def test_constructor_parameters(pod_spec, loop):
    env = {'FOO': 'BAR', 'A': 1}
    with KubeCluster(pod_spec, name='myname', namespace='foo', loop=loop, env=env) as cluster:
        pod = cluster.pod_template
        assert pod.metadata.namespace == 'foo'

        var = [v for v in pod.spec.containers[0].env if v.name == 'FOO']
        assert var and var[0].value == 'BAR'

        var = [v for v in pod.spec.containers[0].env if v.name == 'A']
        assert var and var[0].value == '1'

        assert pod.metadata.generate_name == 'myname'
