import os
from time import sleep, time
import yaml

import dask
import pytest
from dask.distributed import Client, wait
from distributed.utils_test import captured_logger
from dask.utils import tmpfile

from dask_kubernetes.classic import KubeCluster, make_pod_spec
from dask_kubernetes.constants import KUBECLUSTER_CONTAINER_NAME

TEST_DIR = os.path.abspath(os.path.join(__file__, ".."))
CONFIG_DEMO = os.path.join(TEST_DIR, "config-demo.yaml")
FAKE_CERT = os.path.join(TEST_DIR, "fake-cert-file")
FAKE_KEY = os.path.join(TEST_DIR, "fake-key-file")
FAKE_CA = os.path.join(TEST_DIR, "fake-ca-file")


@pytest.fixture
def pod_spec(docker_image):
    yield make_pod_spec(
        image=docker_image, extra_container_config={"imagePullPolicy": "IfNotPresent"}
    )


@pytest.fixture
def cluster(pod_spec):
    with KubeCluster(pod_spec) as cluster:
        yield cluster


@pytest.fixture
def client(cluster):
    with Client(cluster) as client:
        yield client


def test_fixtures(client, cluster):
    client.scheduler_info()
    cluster.scale(1)
    assert client.submit(lambda x: x + 1, 10).result() == 11


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


def test_env(pod_spec):
    with KubeCluster(pod_spec, env={"ABC": "DEF"}) as cluster:
        cluster.scale(1)
        with Client(cluster) as client:
            while not cluster.scheduler_info["workers"]:
                sleep(0.1)
            env = client.run(lambda: dict(os.environ))
            assert all(v["ABC"] == "DEF" for v in env.values())


def dont_test_pod_template_yaml(docker_image):
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
                    "image": docker_image,
                    "imagePullPolicy": "IfNotPresent",
                    "name": KUBECLUSTER_CONTAINER_NAME,
                }
            ]
        },
    }

    with tmpfile(extension="yaml") as fn:
        with open(fn, mode="w") as f:
            yaml.dump(test_yaml, f)
        with KubeCluster(f.name) as cluster:
            cluster.scale(2)
            with Client(cluster) as client:
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


def test_pod_template_yaml_expand_env_vars(docker_image):
    try:
        os.environ["FOO_IMAGE"] = docker_image

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
                        "name": KUBECLUSTER_CONTAINER_NAME,
                    }
                ]
            },
        }

        with tmpfile(extension="yaml") as fn:
            with open(fn, mode="w") as f:
                yaml.dump(test_yaml, f)
            with KubeCluster(f.name) as cluster:
                assert cluster.pod_template.spec.containers[0].image == docker_image
    finally:
        del os.environ["FOO_IMAGE"]


def test_pod_template_dict(docker_image):
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
                    "image": docker_image,
                    "imagePullPolicy": "IfNotPresent",
                    "name": KUBECLUSTER_CONTAINER_NAME,
                }
            ]
        },
    }

    with KubeCluster(spec) as cluster:
        cluster.scale(2)
        with Client(cluster) as client:
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


def test_pod_template_minimal_dict(docker_image):
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
                    "image": docker_image,
                    "imagePullPolicy": "IfNotPresent",
                    "name": KUBECLUSTER_CONTAINER_NAME,
                }
            ]
        }
    }

    with KubeCluster(spec) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = future.result()
            assert result == 11


def test_worker_pod_template_spec_are_copied(docker_image):
    worker_spec = make_pod_spec(docker_image)
    worker_spec.spec.containers[0].args[0] = "fake-worker-cmd"

    with KubeCluster(pod_template=worker_spec):
        assert worker_spec.spec.containers[0].args[0] == "fake-worker-cmd"


def test_scheduler_pod_template_spec_are_copied(docker_image):
    scheduler_spec = make_pod_spec(docker_image)
    scheduler_spec.spec.containers[0].args[0] = "fake-scheduler-cmd"

    with KubeCluster(
        pod_template=make_pod_spec(docker_image), scheduler_pod_template=scheduler_spec
    ):
        assert scheduler_spec.spec.containers[0].args[0] == "fake-scheduler-cmd"


def test_pod_template_from_conf(docker_image):
    spec = {
        "spec": {
            "containers": [{"name": KUBECLUSTER_CONTAINER_NAME, "image": docker_image}]
        }
    }

    with dask.config.set({"kubernetes.worker-template": spec}):
        with KubeCluster() as cluster:
            assert (
                cluster.pod_template.spec.containers[0].name
                == KUBECLUSTER_CONTAINER_NAME
            )


def test_pod_template_with_custom_container_name(docker_image):
    container_name = "my-custom-container"
    spec = {"spec": {"containers": [{"name": container_name, "image": docker_image}]}}

    with dask.config.set({"kubernetes.worker-template": spec}):
        with KubeCluster() as cluster:
            assert cluster.pod_template.spec.containers[0].name == container_name


def test_bad_args():
    with pytest.raises(FileNotFoundError) as info:
        KubeCluster("myfile.yaml")

    with pytest.raises((ValueError, TypeError, AttributeError)) as info:
        KubeCluster({"kind": "Pod"})


def test_constructor_parameters(pod_spec):
    env = {"FOO": "BAR", "A": 1}
    with KubeCluster(pod_spec, name="myname", env=env) as cluster:
        pod = cluster.pod_template

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
        assert time() < start + 30

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
        assert time() < start + 60

    # assert set(cluster.scheduler_info["workers"]) == {b}


def test_automatic_startup(docker_image):
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
                    "image": docker_image,
                    "name": KUBECLUSTER_CONTAINER_NAME,
                }
            ]
        },
    }

    with tmpfile(extension="yaml") as fn:
        with open(fn, mode="w") as f:
            yaml.dump(test_yaml, f)
        with dask.config.set({"kubernetes.worker-template-path": fn}):
            with KubeCluster() as cluster:
                assert cluster.pod_template.metadata.labels["foo"] == "bar"


def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert "Box" not in text
        assert (
            cluster.scheduler.address in text
            or cluster.scheduler.external_address in text
        )
        assert "workers=0" in text


def test_escape_username(pod_spec, monkeypatch):
    monkeypatch.setenv("LOGNAME", "Foo!")

    with KubeCluster(pod_spec) as cluster:
        assert "foo" in cluster.name
        assert "!" not in cluster.name
        assert "foo" in cluster.pod_template.metadata.labels["user"]


def test_escape_name(pod_spec):
    with KubeCluster(pod_spec, name="foo@bar") as cluster:
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


def test_extra_pod_config(docker_image):
    """
    Test that our pod config merging process works fine
    """
    with KubeCluster(
        make_pod_spec(
            docker_image, extra_pod_config={"automountServiceAccountToken": False}
        ),
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.automount_service_account_token is False


def test_extra_container_config(docker_image):
    """
    Test that our container config merging process works fine
    """
    with KubeCluster(
        make_pod_spec(
            docker_image,
            extra_container_config={
                "imagePullPolicy": "IfNotPresent",
                "securityContext": {"runAsUser": 0},
            },
        ),
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.containers[0].image_pull_policy == "IfNotPresent"
        assert pod.spec.containers[0].security_context == {"runAsUser": 0}


def test_container_resources_config(docker_image):
    """
    Test container resource requests / limits being set properly
    """
    with KubeCluster(
        make_pod_spec(
            docker_image, memory_request="0.5G", memory_limit="1G", cpu_limit="1"
        ),
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.containers[0].resources.requests["memory"] == "0.5G"
        assert pod.spec.containers[0].resources.limits["memory"] == "1G"
        assert pod.spec.containers[0].resources.limits["cpu"] == "1"
        assert "cpu" not in pod.spec.containers[0].resources.requests


def test_extra_container_config_merge(docker_image):
    """
    Test that our container config merging process works recursively fine
    """
    with KubeCluster(
        make_pod_spec(
            docker_image,
            env={"TEST": "HI"},
            extra_container_config={
                "env": [{"name": "BOO", "value": "FOO"}],
                "args": ["last-item"],
            },
        ),
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        for e in [{"name": "TEST", "value": "HI"}, {"name": "BOO", "value": "FOO"}]:
            assert e in pod.spec.containers[0].env

        assert pod.spec.containers[0].args[-1] == "last-item"


def test_worker_args(docker_image):
    """
    Test that dask-worker arguments are added to the container args
    """
    with KubeCluster(
        make_pod_spec(
            docker_image,
            memory_limit="5000M",
            resources="FOO=1 BAR=2",
        ),
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        for arg in ["--memory-limit", "5000M", "--resources", "FOO=1 BAR=2"]:
            assert arg in pod.spec.containers[0].args
