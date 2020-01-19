import yaml

import pytest

import dask
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


def test_pod_from_yaml_expand_env_vars(monkeypatch, tmp_path):
    image_name = "foo.jpg"

    monkeypatch.setenv("FOO_IMAGE", image_name)

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

    f = tmp_path / "template.yaml"
    f.write_text(yaml.dump(test_yaml))

    cluster = KubeCluster.from_yaml(str(f), asynchronous=True)

    assert cluster.rendered_worker_pod_template.spec.containers[0].image == image_name


def test_pod_template_from_conf(cluster):
    spec = {"spec": {"containers": [{"name": "some-name", "image": image_name}]}}

    cluster.pod_template = None
    with dask.config.set({"kubernetes.worker-template": spec}):
        assert (
            cluster.rendered_worker_pod_template.spec.containers[0].name == "some-name"
        )


def test_pod_from_config_template_path(tmp_path):
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

    f = tmp_path / "template.yaml"
    f.write_text(yaml.dump(test_yaml))

    with dask.config.set({"kubernetes.worker-template-path": str(f)}):
        cluster = KubeCluster(asynchronous=True)
        assert cluster.rendered_worker_pod_template.metadata.labels["foo"] == "bar"
