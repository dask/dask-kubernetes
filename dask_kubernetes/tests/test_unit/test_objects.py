from dask_kubernetes.objects import (
    make_pod_spec,
    make_pod_from_dict,
    clean_pod_template,
)

image_name = "lolcat.gif"


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
