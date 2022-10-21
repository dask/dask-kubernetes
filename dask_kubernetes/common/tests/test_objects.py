from dask_kubernetes.constants import KUBECLUSTER_CONTAINER_NAME
from dask_kubernetes.common.objects import make_pod_from_dict


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
