import getpass
import os
from time import sleep, time
import uuid
import yaml

import pytest
from daskernetes import KubeCluster
from daskernetes.objects import make_pod_spec
from dask.distributed import Client
from distributed.utils_test import loop, inc  # noqa: F401
from distributed.utils import tmpfile
import kubernetes


try:
    kubernetes.config.load_incluster_config()
except kubernetes.config.ConfigException:
    kubernetes.config.load_kube_config()

api = kubernetes.client.CoreV1Api()


@pytest.fixture
def ns():
    name = 'test-daskernetes'
    try:
        while api.list_namespaced_pod(name).items:  # wait for old pods to clear
            sleep(0.5)
        yield name
    finally:
        pods = api.list_namespaced_pod(name)
        try:
            for pod in pods.items:
                api.delete_namespaced_pod(
                    pod.metadata.name,
                    name,
                    kubernetes.client.V1DeleteOptions()
                )
        except kubernetes.client.rest.ApiException:
            pass


@pytest.fixture
def ns():
    name = 'test-daskernetes' + str(uuid.uuid4())[:10]
    ns = kubernetes.client.V1Namespace(metadata=kubernetes.client.V1ObjectMeta(name=name))
    api.create_namespace(ns)
    try:
        yield name
    finally:
        api.delete_namespace(name, kubernetes.client.V1DeleteOptions())


@pytest.fixture
def pod_spec(image_name):
    yield make_pod_spec(image=image_name)


@pytest.fixture
def cluster(pod_spec, ns, loop):
    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        yield cluster



def test_basic(cluster):
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


def test_logs(cluster):
    cluster.scale_up(2)

    start = time()
    while len(cluster.scheduler.workers) < 2:
        sleep(0.1)
        assert time() < start + 20

    a, b = cluster.pods()
    logs = cluster.logs(a)
    assert 'distributed.worker' in logs


def test_ipython_display(cluster):
    ipywidgets = pytest.importorskip('ipywidgets')
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


def test_namespace(pod_spec, loop, ns):
    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        assert 'dask' in cluster.name
        assert getpass.getuser() in cluster.name
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster2:
            assert cluster.name != cluster2.name

            cluster2.scale_up(1)
            [pod] = cluster2.pods()


def test_adapt(cluster):
    cluster.adapt()
    with Client(cluster) as client:
        future = client.submit(inc, 10)
        result = future.result()
        assert result == 11

    start = time()
    while cluster.scheduler.workers:
        sleep(0.1)
        assert time() < start + 10


def test_env(pod_spec, loop, ns):
    with KubeCluster(pod_spec, env={'ABC': 'DEF'}, loop=loop, namespace=ns) as cluster:
        cluster.scale_up(1)
        with Client(cluster) as client:
            while not cluster.scheduler.workers:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v['ABC'] == 'DEF' for v in env.values())


def test_pod_from_yaml(image_name, loop, ns):
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
        with KubeCluster.from_yaml(f.name, loop=loop, namespace=ns) as cluster:
            assert cluster.namespace == ns
            cluster.scale_up(2)
            with Client(cluster) as client:
                future = client.submit(inc, 10)
                try:
                    result = future.result(timeout=10)
                except Exception as e:
                    import pdb; pdb.set_trace()
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


def test_pod_from_dict(image_name, loop, ns):
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

    with KubeCluster.from_dict(spec, loop=loop, namespace=ns) as cluster:
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


def test_constructor_parameters(pod_spec, loop, ns):
    env = {'FOO': 'BAR', 'A': 1}
    with KubeCluster(pod_spec, name='myname', namespace=ns, loop=loop, env=env) as cluster:
        pod = cluster.pod_template
        assert pod.metadata.namespace == ns

        var = [v for v in pod.spec.containers[0].env if v.name == 'FOO']
        assert var and var[0].value == 'BAR'

        var = [v for v in pod.spec.containers[0].env if v.name == 'A']
        assert var and var[0].value == '1'

        assert pod.metadata.generate_name == 'myname'
