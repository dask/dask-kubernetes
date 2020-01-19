import asyncio
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
def pod_spec(image_name):
    yield make_pod_spec(
        image=image_name, extra_container_config={"imagePullPolicy": "IfNotPresent"}
    )


@pytest.fixture
def cluster(pod_spec, ns):
    with KubeCluster(pod_spec, namespace=ns) as cluster:
        yield cluster


@pytest.fixture
def client(cluster):
    with Client(cluster) as client:
        yield client


def test_fixtures(client, cluster):
    client.scheduler_info()
    cluster.scale(1)
    assert client.submit(lambda x: x + 1, 10).result(timeout=10) == 11


def test_basic(cluster, client):
    cluster.scale(2)
    future = client.submit(lambda x: x + 1, 10)
    result = future.result()
    assert result == 11

    while len(cluster.scheduler_info["workers"]) < 2:
        sleep(0.1)

    # Ensure that inter-worker communication works well
    futures = client.map(lambda x: x + 1, range(10))
    total = client.submit(sum, futures)
    assert total.result() == sum(map(lambda x: x + 1, range(10)))
    assert all(client.has_what().values())


@pytest.mark.xfail(reason="The widget has changed upstream")
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
        assert time() < start + 20
        sleep(0.5)


def test_env(pod_spec, loop, ns):
    with KubeCluster(pod_spec, env={"ABC": "DEF"}, loop=loop, namespace=ns) as cluster:
        cluster.scale(1)
        with Client(cluster, loop=loop) as client:
            while not cluster.scheduler_info["workers"]:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v["ABC"] == "DEF" for v in env.values())


def dont_test_pod_from_yaml(image_name, loop, ns):
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
            with Client(cluster, loop=loop) as client:
                future = client.submit(lambda x: x + 1, 10)
                result = future.result(timeout=10)
                assert result == 11

                start = time()
                while len(cluster.scheduler_info["workers"]) < 2:
                    sleep(0.1)
                    assert time() < start + 20, "timeout"

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
        with Client(cluster, loop=loop) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11

            while len(cluster.scheduler_info["workers"]) < 2:
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
        with Client(cluster, loop=loop) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11


def test_pod_template_from_conf(image_name):
    spec = {"spec": {"containers": [{"name": "some-name", "image": image_name}]}}

    with dask.config.set({"kubernetes.worker-template": spec}):
        with KubeCluster() as cluster:
            assert cluster.pod_template.spec.containers[0].name == "some-name"


def test_bad_args():
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


def test_scale_up_down(cluster, client):
    np = pytest.importorskip("numpy")
    cluster.scale(2)

    start = time()
    while len(cluster.scheduler_info["workers"]) != 2:
        sleep(0.1)
        assert time() < start + 10

    a, b = list(cluster.scheduler_info["workers"])
    x = client.submit(np.ones, 1, workers=a)
    y = client.submit(np.ones, 50_000, workers=b)

    wait([x, y])

    # start = time()
    # while (
    #     cluster.scheduler_info["workers"][a].metrics["memory"]
    #     > cluster.scheduler_info["workers"][b].metrics["memory"]
    # ):
    #     sleep(0.1)
    #     assert time() < start + 1

    cluster.scale(1)

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        sleep(0.1)
        assert time() < start + 20

    # assert set(cluster.scheduler_info["workers"]) == {b}


def test_automatic_startup(image_name, ns):
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
            with KubeCluster(namespace=ns) as cluster:
                assert cluster.pod_template.metadata.labels["foo"] == "bar"


def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert "Box" not in text
        assert (
            cluster.scheduler.address in text
            or cluster.scheduler.external_address in text
        )
        assert "workers=0" in text


def test_escape_username(pod_spec, ns, monkeypatch):
    monkeypatch.setenv("LOGNAME", "foo!")

    with KubeCluster(pod_spec, namespace=ns) as cluster:
        assert "foo" in cluster.name
        assert "!" not in cluster.name
        assert "foo" in cluster.pod_template.metadata.labels["user"]


def test_escape_name(pod_spec, ns):
    with KubeCluster(pod_spec, namespace=ns, name="foo@bar") as cluster:
        assert "@" not in str(cluster.pod_template)


def test_maximum(cluster):
    with dask.config.set({"kubernetes.count.max": 1}):
        with captured_logger("dask_kubernetes") as logger:
            cluster.scale(10)

            start = time()
            while len(cluster.scheduler_info["workers"]) <= 0:
                sleep(0.1)
                assert time() < start + 60

            sleep(0.5)
            assert len(cluster.scheduler_info["workers"]) == 1

        result = logger.getvalue()
        assert "scale beyond maximum number of workers" in result.lower()
