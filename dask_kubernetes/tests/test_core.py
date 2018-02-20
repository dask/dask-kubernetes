import getpass
import os
from time import sleep, time
import uuid
import yaml

import pytest
from distributed.config import set_config
from dask_kubernetes import KubeCluster, make_pod_spec
from dask.distributed import Client, wait
from distributed.utils_test import loop  # noqa: F401
from distributed.utils import tmpfile
import kubernetes


try:
    kubernetes.config.load_incluster_config()
except kubernetes.config.ConfigException:
    kubernetes.config.load_kube_config()

api = kubernetes.client.CoreV1Api()


@pytest.fixture
def ns():
    name = 'test-dask-kubernetes' + str(uuid.uuid4())[:10]
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


@pytest.fixture
def client(cluster):
    with Client(cluster) as client:
        yield client


def test_versions(client):
    client.get_versions(check=True)


def test_basic(cluster, client):
    cluster.scale(2)
    future = client.submit(lambda x: x + 1, 10)
    result = future.result()
    assert result == 11

    while len(cluster.scheduler.workers) < 2:
        sleep(0.1)

    # Ensure that inter-worker communication works well
    futures = client.map(lambda x: x + 1, range(10))
    total = client.submit(sum, futures)
    assert total.result() == sum(map(lambda x: x + 1, range(10)))
    assert all(client.has_what().values())


def test_logs(cluster):
    cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) < 2:
        sleep(0.1)
        assert time() < start + 20

    a, b = cluster.pods()
    logs = cluster.logs(a)
    assert 'distributed.worker' in logs


def test_ipython_display(cluster):
    ipywidgets = pytest.importorskip('ipywidgets')
    cluster.scale(1)
    cluster._ipython_display_()
    box = cluster._cached_widget
    assert isinstance(box, ipywidgets.Widget)
    cluster._ipython_display_()
    assert cluster._cached_widget is box

    start = time()
    while "<td>1</td>" not in str(box):  # one worker in a table
        assert time() < start + 10
        sleep(0.5)


def test_dask_worker_name_env_variable(pod_spec, loop, ns):
    with set_config(**{'kubernetes-worker-name': 'foo-{USER}-{uuid}'}):
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
            assert 'foo-' + getpass.getuser() in cluster.name


def test_diagnostics_link_env_variable(pod_spec, loop, ns):
    pytest.importorskip('bokeh')
    with set_config(**{'diagnostics-link': 'foo-{USER}-{port}'}):
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
            port = cluster.scheduler.services['bokeh'].port
            cluster._ipython_display_()
            box = cluster._cached_widget

            assert 'foo-' + getpass.getuser() + '-' + str(port) in str(box)


def test_namespace(pod_spec, loop, ns):
    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        assert 'dask' in cluster.name
        assert getpass.getuser() in cluster.name
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster2:
            assert cluster.name != cluster2.name

            cluster2.scale(1)
            [pod] = cluster2.pods()


def test_adapt(cluster):
    cluster.adapt()
    with Client(cluster) as client:
        future = client.submit(lambda x: x + 1, 10)
        result = future.result()
        assert result == 11

    start = time()
    while cluster.scheduler.workers:
        sleep(0.1)
        assert time() < start + 10


def test_env(pod_spec, loop, ns):
    with KubeCluster(pod_spec, env={'ABC': 'DEF'}, loop=loop, namespace=ns) as cluster:
        cluster.scale(1)
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
            cluster.scale(2)
            with Client(cluster) as client:
                future = client.submit(lambda x: x + 1, 10)
                result = future.result(timeout=10)
                assert result == 11

                start = time()
                while len(cluster.scheduler.workers) < 2:
                    sleep(0.1)
                    assert time() < start + 10, 'timeout'

                # Ensure that inter-worker communication works well
                futures = client.map(lambda x: x + 1, range(10))
                total = client.submit(sum, futures)
                assert total.result() == sum(map(lambda x: x + 1, range(10)))
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
        cluster.scale(2)
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11

            while len(cluster.scheduler.workers) < 2:
                sleep(0.1)

            # Ensure that inter-worker communication works well
            futures = client.map(lambda x: x + 1, range(10))
            total = client.submit(sum, futures)
            assert total.result() == sum(map(lambda x: x + 1, range(10)))
            assert all(client.has_what().values())


def test_pod_from_minimal_dict(image_name, loop, ns):
    spec = {
        'spec': {
            'containers': [{
                'args': ['dask-worker', '$(DASK_SCHEDULER_ADDRESS)',
                         '--nthreads', '1',
                         '--death-timeout', '60'],
                'command': None,
                'image': image_name,
                'name': 'worker'
            }]
        }
    }

    with KubeCluster.from_dict(spec, loop=loop, namespace=ns) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11


def test_bad_args(loop):
    with pytest.raises(TypeError) as info:
        KubeCluster('myfile.yaml')

    assert 'KubeCluster.from_yaml' in str(info.value)

    with pytest.raises(TypeError) as info:
        KubeCluster({})

    assert 'KubeCluster.from_dict' in str(info.value)


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


def test_scale_up_down(cluster, client):
    np = pytest.importorskip('numpy')
    cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) != 2:
        sleep(0.1)
        assert time() < start + 10

    a, b = list(cluster.scheduler.workers)
    x = client.submit(np.ones, 1, workers=a)
    y = client.submit(np.ones, 100_000_000, workers=b)

    wait([x, y])

    start = time()
    while (cluster.scheduler.workers[a].info['memory'] >
           cluster.scheduler.workers[b].info['memory']):
        sleep(0.1)
        assert time() < start + 1

    cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        sleep(0.1)
        assert time() < start + 10

    assert set(cluster.scheduler.workers) == {b}


def test_automatic_startup(image_name, loop, ns):
    test_yaml = {
        "kind": "Pod",
        "metadata": {
            "labels": {
                "foo": "bar",
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
        with set_config(**{'kubernetes-worker-template-path': fn}):
            with KubeCluster(loop=loop, namespace=ns) as cluster:
                assert cluster.pod_template.metadata.labels['foo'] == 'bar'


def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert 'Box' not in text
        assert cluster.scheduler.address in text
        assert "workers=0" in text
