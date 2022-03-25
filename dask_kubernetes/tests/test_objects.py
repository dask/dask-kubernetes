from dask_kubernetes import KubeCluster
from dask_kubernetes.constants import KUBECLUSTER_WORKER_CONTAINER_NAME
from dask_kubernetes.objects import make_pod_spec, make_pod_from_dict
from distributed.utils_test import loop  # noqa: F401


def test_extra_pod_config(docker_image, loop):
    """
    Test that our pod config merging process works fine
    """
    with KubeCluster(
        make_pod_spec(
            docker_image, extra_pod_config={"automountServiceAccountToken": False}
        ),
        loop=loop,
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.automount_service_account_token is False


def test_extra_container_config(docker_image, loop):
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
        loop=loop,
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.containers[0].image_pull_policy == "IfNotPresent"
        assert pod.spec.containers[0].security_context == {"runAsUser": 0}


def test_container_resources_config(docker_image, loop):
    """
    Test container resource requests / limits being set properly
    """
    with KubeCluster(
        make_pod_spec(
            docker_image, memory_request="0.5G", memory_limit="1G", cpu_limit="1"
        ),
        loop=loop,
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        assert pod.spec.containers[0].resources.requests["memory"] == "0.5G"
        assert pod.spec.containers[0].resources.limits["memory"] == "1G"
        assert pod.spec.containers[0].resources.limits["cpu"] == "1"
        assert "cpu" not in pod.spec.containers[0].resources.requests


def test_extra_container_config_merge(docker_image, loop):
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
        loop=loop,
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        for e in [{"name": "TEST", "value": "HI"}, {"name": "BOO", "value": "FOO"}]:
            assert e in pod.spec.containers[0].env

        assert pod.spec.containers[0].args[-1] == "last-item"


def test_worker_args(docker_image, loop):
    """
    Test that dask-worker arguments are added to the container args
    """
    with KubeCluster(
        make_pod_spec(
            docker_image,
            memory_limit="5000M",
            resources="FOO=1 BAR=2",
        ),
        loop=loop,
        n_workers=0,
    ) as cluster:

        pod = cluster.pod_template

        for arg in ["--memory-limit", "5000M", "--resources", "FOO=1 BAR=2"]:
            assert arg in pod.spec.containers[0].args


def test_make_pod_from_dict():
    d = {
        "kind": "Pod",
        "metadata": {"labels": {"app": "dask", "dask.org/component": "dask-worker"}},
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                    ],
                    "image": "image-name",
                    "name": KUBECLUSTER_WORKER_CONTAINER_NAME,
                    "securityContext": {
                        "capabilities": {"add": ["SYS_ADMIN"]},
                        "privileged": True,
                    },
                }
            ],
            "restartPolicy": "Never",
        },
    }

    pod = make_pod_from_dict(d)

    assert pod.spec.restart_policy == "Never"
    assert pod.spec.containers[0].security_context.privileged
    assert pod.spec.containers[0].security_context.capabilities.add == ["SYS_ADMIN"]


def test_make_pod_from_dict_default_container_name():
    d = {
        "kind": "Pod",
        "metadata": {"labels": {"app": "dask", "dask.org/component": "dask-worker"}},
        "spec": {
            "containers": [
                {
                    "args": [
                        "dask-worker",
                        "$(DASK_SCHEDULER_ADDRESS)",
                        "--nthreads",
                        "1",
                    ],
                    "image": "image-name",
                    "securityContext": {
                        "capabilities": {"add": ["SYS_ADMIN"]},
                        "privileged": True,
                    },
                },
                {"image": "image-name2", "name": "sidecar"},
                {"image": "image-name3"},
            ],
            "restartPolicy": "Never",
        },
    }

    pod = make_pod_from_dict(d)
    assert pod.spec.containers[0].name == "dask-0"
    assert pod.spec.containers[1].name == "sidecar"
    assert pod.spec.containers[2].name == "dask-2"
