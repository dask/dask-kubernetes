import pytest

from dask_kubernetes import KubeCluster
from dask_kubernetes.objects import (
    make_pod_spec,
    make_pod_from_dict,
    clean_pod_template,
)


@pytest.fixture
def cluster():
    """Provide a cluster without starting any loops."""
    return KubeCluster(asynchronous=True)


image_name = "lolcat.gif"


def test_extra_pod_config(cluster):
    """
    Test that our pod config merging process works fine
    """
    pod_template = make_pod_spec(
        image_name, extra_pod_config={"automountServiceAccountToken": False}
    )

    cluster.pod_template = pod_template
    pod = cluster.rendered_worker_pod_template

    assert pod.spec.automount_service_account_token is False


def test_extra_container_config(cluster):
    """
    Test that our container config merging process works fine
    """
    pod_template = make_pod_spec(
        image_name,
        extra_container_config={
            "imagePullPolicy": "IfNotPresent",
            "securityContext": {"runAsUser": 0},
        },
    )

    cluster.pod_template = pod_template
    pod = cluster.rendered_worker_pod_template

    assert pod.spec.containers[0].image_pull_policy == "IfNotPresent"
    assert pod.spec.containers[0].security_context == {"runAsUser": 0}


def test_container_resources_config(cluster):
    """
    Test container resource requests / limits being set properly
    """
    pod_template = make_pod_spec(
        image_name, memory_request="0.5G", memory_limit="1G", cpu_limit="1"
    )

    cluster.pod_template = pod_template
    pod = cluster.rendered_worker_pod_template

    assert pod.spec.containers[0].resources.requests["memory"] == "0.5G"
    assert pod.spec.containers[0].resources.limits["memory"] == "1G"
    assert pod.spec.containers[0].resources.limits["cpu"] == "1"
    assert "cpu" not in pod.spec.containers[0].resources.requests


def test_extra_container_config_merge(cluster):
    """
    Test that our container config merging process works recursively fine
    """
    pod_template = make_pod_spec(
        image_name,
        extra_container_config={
            "env": [{"name": "BOO", "value": "FOO"}],
            "args": ["last-item"],
        },
    )

    cluster.pod_template = pod_template
    cluster.env = {"TEST": "HI"}

    pod = cluster.rendered_worker_pod_template

    assert pod.spec.containers[0].env == [
        {"name": "TEST", "value": "HI"},
        {"name": "BOO", "value": "FOO"},
    ]
    assert pod.spec.containers[0].args[-1] == "last-item"


def test_extra_container_config_merge(cluster):
    """
    Test that our container config merging process works recursively fine
    """
    pod_template = make_pod_spec(
        image_name,
        env={"TEST": "HI"},
        extra_container_config={
            "env": [{"name": "BOO", "value": "FOO"}],
            "args": ["last-item"],
        },
    )

    cluster.pod_template = pod_template
    pod = cluster.rendered_worker_pod_template

    for e in [{"name": "TEST", "value": "HI"}, {"name": "BOO", "value": "FOO"}]:
        assert e in pod.spec.containers[0].env

    assert pod.spec.containers[0].args[-1] == "last-item"


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
                    "name": "dask-worker",
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


def test_default_toleration():
    pod_spec = clean_pod_template(make_pod_spec(image=image_name))
    tolerations = pod_spec.to_dict()["spec"]["tolerations"]
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


def test_default_toleration_preserved():
    pod_spec = clean_pod_template(
        make_pod_spec(
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
    )
    tolerations = pod_spec.to_dict()["spec"]["tolerations"]
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
