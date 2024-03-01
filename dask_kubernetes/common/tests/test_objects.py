import pytest

from dask_kubernetes.common.objects import make_pod_from_dict, validate_cluster_name
from dask_kubernetes.constants import KUBECLUSTER_CONTAINER_NAME, MAX_CLUSTER_NAME_LEN
from dask_kubernetes.exceptions import ValidationError


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
                    "name": KUBECLUSTER_CONTAINER_NAME,
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


@pytest.mark.parametrize(
    "cluster_name",
    [
        (MAX_CLUSTER_NAME_LEN + 1) * "a",
        "invalid.chars.in.name",
    ],
)
def test_validate_cluster_name_raises_on_invalid_name(
    cluster_name,
):

    with pytest.raises(ValidationError):
        validate_cluster_name(cluster_name)


def test_validate_cluster_name_success_on_valid_name():
    assert validate_cluster_name("valid-cluster-name-123") is None
