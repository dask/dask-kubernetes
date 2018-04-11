from dask_kubernetes import KubeCluster
from dask_kubernetes.objects import make_pod_spec, make_pod_from_dict
from distributed.utils_test import loop  # noqa: F401


def test_extra_pod_config(image_name, loop):
    """
    Test that our pod config merging process works fine
    """
    cluster = KubeCluster(
        make_pod_spec(
            image_name,
            extra_pod_config={
                'automountServiceAccountToken': False
            }
        ),
        loop=loop,
        n_workers=0,
    )

    pod = cluster.pod_template

    assert pod.spec.automount_service_account_token is False


def test_extra_container_config(image_name, loop):
    """
    Test that our container config merging process works fine
    """
    cluster = KubeCluster(
        make_pod_spec(
            image_name,
            extra_container_config={
                'imagePullPolicy': 'IfNotPresent',
                'securityContext': {
                    'runAsUser': 0
                }
            }
        ),
        loop=loop,
        n_workers=0,
    )

    pod = cluster.pod_template

    assert pod.spec.containers[0].image_pull_policy == 'IfNotPresent'
    assert pod.spec.containers[0].security_context == {
        'runAsUser': 0
    }


def test_container_resources_config(image_name, loop):
    """
    Test container resource requests / limits being set properly
    """
    cluster = KubeCluster(
        make_pod_spec(
            image_name,
            memory_request="1G",
            memory_limit="2G",
            cpu_limit="2",
        ),
        loop=loop,
        n_workers=0,
    )

    pod = cluster.pod_template

    assert pod.spec.containers[0].resources.requests['memory'] == '1G'
    assert pod.spec.containers[0].resources.limits['memory'] == '2G'
    assert pod.spec.containers[0].resources.limits['cpu'] == '2'
    assert "cpu" not in pod.spec.containers[0].resources.requests


def test_extra_container_config_merge(image_name, loop):
    """
    Test that our container config merging process works recursively fine
    """
    cluster = KubeCluster(
        make_pod_spec(
            image_name,
            extra_container_config={
                "env": [ {"name": "BOO", "value": "FOO" } ],
                "args": ["last-item"]
            }
        ),
        loop=loop,
        n_workers=0,
        env={"TEST": "HI"},
    )

    pod = cluster.pod_template

    assert pod.spec.containers[0].env == [
        { "name": "TEST", "value": "HI"},
        { "name": "BOO", "value": "FOO"}
    ]

    assert pod.spec.containers[0].args[-1] == "last-item"


def test_extra_container_config_merge(image_name, loop):
    """
    Test that our container config merging process works recursively fine
    """
    cluster = KubeCluster(
        make_pod_spec(
            image_name,
            env={"TEST": "HI"},
            extra_container_config={
                "env": [ {"name": "BOO", "value": "FOO" } ],
                "args": ["last-item"]
            }
        ),
        loop=loop,
        n_workers=0,
    )

    pod = cluster.pod_template

    for e in [
        { "name": "TEST", "value": "HI"},
        { "name": "BOO", "value": "FOO"}
    ]:
        assert e in pod.spec.containers[0].env

    assert pod.spec.containers[0].args[-1] == "last-item"


def test_make_pod_from_dict():
    d = {
        "kind": "Pod",
        "metadata": {
            "labels": {
                "app": "dask",
                "component": "dask-worker"
            }
        },
        "spec": {
            "containers": [{
                "args": [
                    "dask-worker",
                    "$(DASK_SCHEDULER_ADDRESS)",
                    "--nthreads",
                    "1"
                ],
                "image": "image-name",
                "name": "dask-worker",
                "securityContext": {"capabilities": {"add": ["SYS_ADMIN"]},
                                     "privileged": True},
            }],
            "restartPolicy": "Never",
        }
    }

    pod = make_pod_from_dict(d)

    assert pod.spec.restart_policy == 'Never'
    assert pod.spec.containers[0].security_context.privileged
    assert pod.spec.containers[0].security_context.capabilities.add == ['SYS_ADMIN']
