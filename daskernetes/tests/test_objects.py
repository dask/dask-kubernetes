import pytest
from daskernetes import KubeCluster
from distributed.utils_test import loop


def test_extra_pod_config(loop):
    """
    Test that our pod config merging process works fine
    """
    cluster = KubeCluster(
        loop=loop,
        n_workers=0,
        extra_pod_config={
            'automountServiceAccountToken': False
        }
    )

    pod = cluster._make_pod()

    assert pod.spec.automount_service_account_token == False

def test_extra_container_config(loop):
    """
    Test that our container config merging process works fine
    """
    cluster = KubeCluster(
        loop=loop,
        n_workers=0,
        extra_container_config={
            'imagePullPolicy': 'IfNotReady',
            'securityContext': {
                'runAsUser': 0
            }
        }
    )

    pod = cluster._make_pod()

    assert pod.spec.containers[0].image_pull_policy == 'IfNotReady'
    assert pod.spec.containers[0].security_context == {
        'runAsUser': 0
    }

def test_container_resources_config(loop):
    """
    Test container resource requests / limits being set properly
    """
    cluster = KubeCluster(
        loop=loop,
        n_workers=0,
        memory_request="1G",
        memory_limit="2G",
        cpu_limit="2"
    )

    pod = cluster._make_pod()

    assert pod.spec.containers[0].resources.requests['memory'] == '1G'
    assert pod.spec.containers[0].resources.limits['memory'] == '2G'
    assert pod.spec.containers[0].resources.limits['cpu'] == '2'
    assert "cpu" not in pod.spec.containers[0].resources.requests

def test_extra_container_config_merge(loop):
    """
    Test that our container config merging process works recursively fine
    """
    cluster = KubeCluster(
        loop=loop,
        n_workers=0,
        env={"TEST": "HI"},
        extra_container_config={
            "env": [ {"name": "BOO", "value": "FOO" } ],
            "args": ["last-item"]
        }
    )

    pod = cluster._make_pod()

    assert pod.spec.containers[0].env == [
        { "name": "TEST", "value": "HI"},
        { "name": "BOO", "value": "FOO"}
    ]

    assert pod.spec.containers[0].args[-1] == "last-item"


def test_extra_container_config_merge(loop):
    """
    Test that our container config merging process works recursively fine
    """
    cluster = KubeCluster(
        loop=loop,
        n_workers=0,
        env={"TEST": "HI"},
        extra_container_config={
            "env": [ {"name": "BOO", "value": "FOO" } ],
            "args": ["last-item"]
        }
    )

    pod = cluster._make_pod()

    assert pod.spec.containers[0].env == [
        { "name": "TEST", "value": "HI"},
        { "name": "BOO", "value": "FOO"}
    ]

    assert pod.spec.containers[0].args[-1] == "last-item"
