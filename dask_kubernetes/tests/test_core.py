import base64
import getpass
import os
from time import sleep, time
import uuid
import yaml

import dask
import pytest
from dask_kubernetes import (
    KubeCluster,
    make_pod_spec,
    ClusterAuth,
    KubeConfig,
    KubeAuth,
)
from dask_kubernetes.objects import clean_pod_template
from dask.distributed import Client, wait
from distributed.utils_test import loop, captured_logger  # noqa: F401
from distributed.utils import tmpfile
import kubernetes
from random import random

TEST_DIR = os.path.abspath(os.path.join(__file__, ".."))
CONFIG_DEMO = os.path.join(TEST_DIR, "config-demo.yaml")
FAKE_CERT = os.path.join(TEST_DIR, "fake-cert-file")
FAKE_KEY = os.path.join(TEST_DIR, "fake-key-file")
FAKE_CA = os.path.join(TEST_DIR, "fake-ca-file")


@pytest.fixture
def api():
    ClusterAuth.load_first()
    return kubernetes.client.CoreV1Api()


@pytest.fixture
def ns(api):
    name = "test-dask-kubernetes" + str(uuid.uuid4())[:10]
    ns = kubernetes.client.V1Namespace(
        metadata=kubernetes.client.V1ObjectMeta(name=name)
    )
    api.create_namespace(ns)
    try:
        yield name
    finally:
        api.delete_namespace(name)


@pytest.fixture
def pod_spec(image_name):
    yield make_pod_spec(
        image=image_name, extra_container_config={"imagePullPolicy": "IfNotPresent"}
    )


@pytest.fixture
def clean_pod_spec(pod_spec):
    yield clean_pod_template(pod_spec)


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
    assert "distributed.worker" in logs

    logs = cluster.logs()
    assert len(logs) == 2
    for pod in logs:
        assert "distributed.worker" in logs[pod]


def test_ipython_display(cluster):
    ipywidgets = pytest.importorskip("ipywidgets")
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
    with dask.config.set({"kubernetes.name": "foo-{USER}-{uuid}"}):
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
            assert "foo-" + getpass.getuser() in cluster.name


def test_diagnostics_link_env_variable(pod_spec, loop, ns):
    pytest.importorskip("bokeh")
    pytest.importorskip("ipywidgets")
    with dask.config.set({"distributed.dashboard.link": "foo-{USER}-{port}"}):
        with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
            port = cluster.scheduler.services["dashboard"].port
            cluster._ipython_display_()
            box = cluster._cached_widget

            assert "foo-" + getpass.getuser() + "-" + str(port) in str(box)


def test_namespace(pod_spec, loop, ns):
    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        assert "dask" in cluster.name
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
    with KubeCluster(pod_spec, env={"ABC": "DEF"}, loop=loop, namespace=ns) as cluster:
        cluster.scale(1)
        with Client(cluster) as client:
            while not cluster.scheduler.workers:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v["ABC"] == "DEF" for v in env.values())


def test_pod_from_yaml(image_name, loop, ns):
    test_yaml = {
        "kind": "Pod",
        "metadata": {"labels": {"app": "dask", "component": "dask-worker"}},
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                    ],
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "dask-worker",
                }
            ]
        },
    }

    with tmpfile(extension="yaml") as fn:
        with open(fn, mode="w") as f:
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
                    assert time() < start + 10, "timeout"

                # Ensure that inter-worker communication works well
                futures = client.map(lambda x: x + 1, range(10))
                total = client.submit(sum, futures)
                assert total.result() == sum(map(lambda x: x + 1, range(10)))
                assert all(client.has_what().values())


def test_pod_from_yaml_expand_env_vars(image_name, loop, ns):
    try:
        os.environ["FOO_IMAGE"] = image_name

        test_yaml = {
            "kind": "Pod",
            "metadata": {"labels": {"app": "dask", "component": "dask-worker"}},
            "spec": {
                "containers": [
                    {
                        "args": [
                            "dask-worker",
                            "$(DASK_SCHEDULER_ADDRESS)",
                            "--nthreads",
                            "1",
                        ],
                        "image": "${FOO_IMAGE}",
                        "imagePullPolicy": "IfNotPresent",
                        "name": "dask-worker",
                    }
                ]
            },
        }

        with tmpfile(extension="yaml") as fn:
            with open(fn, mode="w") as f:
                yaml.dump(test_yaml, f)
            with KubeCluster.from_yaml(f.name, loop=loop, namespace=ns) as cluster:
                assert cluster.pod_template.spec.containers[0].image == image_name
    finally:
        del os.environ["FOO_IMAGE"]


def test_pod_from_dict(image_name, loop, ns):
    spec = {
        "metadata": {},
        "restartPolicy": "Never",
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                        "--death-timeout",
                        "60",
                    ],
                    "command": None,
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "dask-worker",
                }
            ]
        },
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
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                        "--death-timeout",
                        "60",
                    ],
                    "command": None,
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "worker",
                }
            ]
        }
    }

    with KubeCluster.from_dict(spec, loop=loop, namespace=ns) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11


def test_pod_template_from_conf():
    spec = {"spec": {"containers": [{"name": "some-name"}]}}

    with dask.config.set({"kubernetes.worker-template": spec}):
        with KubeCluster() as cluster:
            assert cluster.pod_template.spec.containers[0].name == "some-name"


def test_bad_args(loop):
    with pytest.raises(TypeError) as info:
        KubeCluster("myfile.yaml")

    assert "KubeCluster.from_yaml" in str(info.value)

    with pytest.raises((ValueError, TypeError)) as info:
        KubeCluster({"kind": "Pod"})

    assert "KubeCluster.from_dict" in str(info.value)


def test_constructor_parameters(pod_spec, loop, ns):
    env = {"FOO": "BAR", "A": 1}
    with KubeCluster(
        pod_spec, name="myname", namespace=ns, loop=loop, env=env
    ) as cluster:
        pod = cluster.pod_template
        assert pod.metadata.namespace == ns

        var = [v for v in pod.spec.containers[0].env if v.name == "FOO"]
        assert var and var[0].value == "BAR"

        var = [v for v in pod.spec.containers[0].env if v.name == "A"]
        assert var and var[0].value == "1"

        assert pod.metadata.generate_name == "myname"


def test_reject_evicted_workers(cluster):
    cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        sleep(0.1)
        assert time() < start + 60

    # Evict worker
    [worker] = cluster.pods()
    cluster.core_api.create_namespaced_pod_eviction(
        worker.metadata.name,
        worker.metadata.namespace,
        kubernetes.client.V1beta1Eviction(
            delete_options=kubernetes.client.V1DeleteOptions(grace_period_seconds=300),
            metadata=worker.metadata,
        ),
    )

    # Wait until pod is evicted
    start = time()
    while cluster.pods()[0].status.phase == "Running":
        sleep(0.1)
        assert time() < start + 60

    [worker] = cluster.pods()
    assert worker.status.phase == "Failed"

    # Make sure the failed pod is removed
    pods = cluster._cleanup_terminated_pods([worker])
    assert len(pods) == 0

    start = time()
    while cluster.pods():
        sleep(0.1)
        assert time() < start + 60


def test_scale_up_down(cluster, client):
    np = pytest.importorskip("numpy")
    cluster.scale(2)

    start = time()
    while len(cluster.scheduler.workers) != 2:
        sleep(0.1)
        assert time() < start + 10

    a, b = list(cluster.scheduler.workers)
    x = client.submit(np.ones, 1, workers=a)
    y = client.submit(np.ones, 50_000_000, workers=b)

    wait([x, y])

    start = time()
    while (
        cluster.scheduler.workers[a].metrics["memory"]
        > cluster.scheduler.workers[b].metrics["memory"]
    ):
        sleep(0.1)
        assert time() < start + 1

    cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        sleep(0.1)
        assert time() < start + 10

    assert set(cluster.scheduler.workers) == {b}


def test_scale_up_down_fast(cluster, client):
    cluster.scale(1)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        sleep(0.1)
        assert time() < start + 10

    worker = next(iter(cluster.scheduler.workers.values()))

    # Put some data on this worker
    future = client.submit(lambda: b"\x00" * int(1e6))
    wait(future)
    assert worker in cluster.scheduler.tasks[future.key].who_has

    # Rescale the cluster many times without waiting: this should put some
    # pressure on kubernetes but this should never fail nor delete our worker
    # with the temporary result.
    for i in range(10):
        cluster.scale(4)
        sleep(random() / 2)
        cluster.scale(1)
        sleep(random() / 2)

    start = time()
    while len(cluster.scheduler.workers) != 1:
        sleep(0.1)
        assert time() < start + 10

    # The original task result is still stored on the original worker: this pod
    # has never been deleted when rescaling the cluster and the result can
    # still be fetched back.
    assert worker in cluster.scheduler.tasks[future.key].who_has
    assert len(future.result()) == int(1e6)


def test_scale_down_pending(cluster, client):
    # Try to scale the cluster to use more pods than available
    nodes = cluster.core_api.list_node().items
    max_pods = sum(int(node.status.allocatable["pods"]) for node in nodes)
    if max_pods > 50:
        # It's probably not reasonable to run this test against a large
        # kubernetes cluster.
        pytest.skip("Require a small test kubernetes cluster (maxpod <= 50)")
    extra_pods = 5
    requested_pods = max_pods + extra_pods
    cluster.scale(requested_pods)

    start = time()
    while len(cluster.scheduler.workers) < 2:
        sleep(0.1)
        # Wait a bit because the kubernetes cluster can take time to provision
        # the requested pods as we requested a large number of pods.
        assert time() < start + 60

    pending_pods = [p for p in cluster.pods() if p.status.phase == "Pending"]
    assert len(pending_pods) >= extra_pods

    running_workers = list(cluster.scheduler.workers.keys())
    assert len(running_workers) >= 2

    # Put some data on those workers to make them important to keep as long
    # as possible.
    def load_data(i):
        return b"\x00" * (i * int(1e6))

    futures = [
        client.submit(load_data, i, workers=w) for i, w in enumerate(running_workers)
    ]
    wait(futures)

    # Reduce the cluster size down to the actually useful nodes: pending pods
    # and running pods without results should be shutdown and removed first:
    cluster.scale(len(running_workers))

    start = time()
    pod_statuses = [p.status.phase for p in cluster.pods()]
    while len(pod_statuses) != len(running_workers):
        if time() - start > 60:
            raise AssertionError(
                "Expected %d running pods but got %r"
                % (len(running_workers), pod_statuses)
            )
        sleep(0.1)
        pod_statuses = [p.status.phase for p in cluster.pods()]

    assert pod_statuses == ["Running"] * len(running_workers)
    assert list(cluster.scheduler.workers.keys()) == running_workers

    # Terminate everything
    cluster.scale(0)

    start = time()
    while len(cluster.scheduler.workers) > 0:
        sleep(0.1)
        assert time() < start + 60


def test_automatic_startup(image_name, loop, ns):
    test_yaml = {
        "kind": "Pod",
        "metadata": {"labels": {"foo": "bar"}},
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                    ],
                    "image": image_name,
                    "name": "dask-worker",
                }
            ]
        },
    }

    with tmpfile(extension="yaml") as fn:
        with open(fn, mode="w") as f:
            yaml.dump(test_yaml, f)
        with dask.config.set({"kubernetes.worker-template-path": fn}):
            with KubeCluster(loop=loop, namespace=ns) as cluster:
                assert cluster.pod_template.metadata.labels["foo"] == "bar"


def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert "Box" not in text
        assert cluster.scheduler.address in text
        assert "workers=0" in text


def test_escape_username(pod_spec, loop, ns, monkeypatch):
    monkeypatch.setenv("LOGNAME", "foo!")

    with KubeCluster(pod_spec, loop=loop, namespace=ns) as cluster:
        assert "foo" in cluster.name
        assert "!" not in cluster.name
        assert "foo" in cluster.pod_template.metadata.labels["user"]


def test_escape_name(pod_spec, loop, ns):
    with KubeCluster(pod_spec, loop=loop, namespace=ns, name="foo@bar") as cluster:
        assert "@" not in str(cluster.pod_template)


def test_maximum(cluster):
    with dask.config.set(**{"kubernetes.count.max": 1}):
        with captured_logger("dask_kubernetes") as logger:
            cluster.scale(10)

            start = time()
            while len(cluster.scheduler.workers) <= 0:
                sleep(0.1)
                assert time() < start + 60

            sleep(0.5)
            assert len(cluster.scheduler.workers) == 1

        result = logger.getvalue()
        assert "scale beyond maximum number of workers" in result.lower()


def test_default_toleration(clean_pod_spec):
    tolerations = clean_pod_spec.to_dict()["spec"]["tolerations"]
    assert {
        "key": "k8s.dask.org/dedicated",
        "operator": "Equal",
        "value": "worker",
        "effect": "NoSchedule",
        "toleration_seconds": None,
    } in tolerations
    assert {
        "key": "k8s.dask.org_dedicated",
        "operator": "Equal",
        "value": "worker",
        "effect": "NoSchedule",
        "toleration_seconds": None,
    } in tolerations


def test_default_toleration_preserved(image_name):
    pod_spec = make_pod_spec(
        image=image_name,
        extra_pod_config={
            "tolerations": [
                {
                    "key": "example.org/toleration",
                    "operator": "Exists",
                    "effect": "NoSchedule",
                }
            ]
        },
    )
    cluster = KubeCluster(pod_spec)
    tolerations = cluster.pod_template.to_dict()["spec"]["tolerations"]
    assert {
        "key": "k8s.dask.org/dedicated",
        "operator": "Equal",
        "value": "worker",
        "effect": "NoSchedule",
        "toleration_seconds": None,
    } in tolerations
    assert {
        "key": "k8s.dask.org_dedicated",
        "operator": "Equal",
        "value": "worker",
        "effect": "NoSchedule",
        "toleration_seconds": None,
    } in tolerations
    assert {
        "key": "example.org/toleration",
        "operator": "Exists",
        "effect": "NoSchedule",
    } in tolerations


def test_default_affinity(clean_pod_spec):
    affinity = clean_pod_spec.to_dict()["spec"]["affinity"]

    assert (
        {"key": "k8s.dask.org/node-purpose", "operator": "In", "values": ["worker"]}
        in affinity["node_affinity"][
            "preferred_during_scheduling_ignored_during_execution"
        ][0]["preference"]["match_expressions"]
    )
    assert (
        affinity["node_affinity"][
            "preferred_during_scheduling_ignored_during_execution"
        ][0]["weight"]
        == 100
    )
    assert (
        affinity["node_affinity"]["required_during_scheduling_ignored_during_execution"]
        is None
    )
    assert affinity["pod_affinity"] is None


def test_auth_missing(pod_spec, ns, loop):
    with pytest.raises(kubernetes.config.ConfigException) as info:
        KubeCluster(pod_spec, auth=[], loop=loop, namespace=ns)

    assert "No authorization methods were provided" in str(info.value)


def test_auth_tries_all_methods(pod_spec, ns, loop):
    fails = {"count": 0}

    class FailAuth(ClusterAuth):
        def load(self):
            fails["count"] += 1
            raise kubernetes.config.ConfigException("Fail #{count}".format(**fails))

    with pytest.raises(kubernetes.config.ConfigException) as info:
        KubeCluster(pod_spec, auth=[FailAuth()] * 3, loop=loop, namespace=ns)

    assert "Fail #3" in str(info.value)
    assert fails["count"] == 3


def test_auth_kubeconfig_with_filename():
    KubeConfig(config_file=CONFIG_DEMO).load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://1.2.3.4"
    assert config.cert_file == FAKE_CERT
    assert config.key_file == FAKE_KEY
    assert config.ssl_ca_cert == FAKE_CA


def test_auth_kubeconfig_with_context():
    KubeConfig(config_file=CONFIG_DEMO, context="exp-scratch").load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://5.6.7.8"
    assert config.api_key["authorization"] == "Basic {}".format(
        base64.b64encode(b"exp:some-password").decode("ascii")
    )


def test_auth_explicit():
    KubeAuth(host="https://9.8.7.6", username="abc", password="some-password").load()

    config = kubernetes.client.Configuration()
    assert config.host == "https://9.8.7.6"
    assert config.username == "abc"
    assert config.password == "some-password"
    assert config.get_basic_auth_token() == "Basic {}".format(
        base64.b64encode(b"abc:some-password").decode("ascii")
    )
