import pytest_asyncio
import asyncio
import base64
import getpass
import os
import random
from time import time
import yaml
import sys

import kubernetes_asyncio as kubernetes
import pytest

import dask
from dask.distributed import Client, wait
import dask_kubernetes
from dask_kubernetes import (
    KubeCluster,
    make_pod_spec,
    clean_pod_template,
    ClusterAuth,
    KubeConfig,
    KubeAuth,
)
from dask.utils import tmpfile
from distributed.utils_test import captured_logger

from dask_kubernetes.constants import KUBECLUSTER_CONTAINER_NAME
from dask_kubernetes.common.utils import get_current_namespace

TEST_DIR = os.path.abspath(os.path.join(__file__, ".."))
CONFIG_DEMO = os.path.join(TEST_DIR, "config-demo.yaml")
FAKE_CERT = os.path.join(TEST_DIR, "fake-cert-file")
FAKE_KEY = os.path.join(TEST_DIR, "fake-key-file")
FAKE_CA = os.path.join(TEST_DIR, "fake-ca-file")


def test_deprecation_warning():
    with pytest.deprecated_call():
        from dask_kubernetes import KubeCluster as TLKubeCluster

    from dask_kubernetes.classic import KubeCluster as ClassicKubeCluster

    assert TLKubeCluster is ClassicKubeCluster


@pytest.fixture
def pod_spec(docker_image):
    yield clean_pod_template(
        make_pod_spec(
            image=docker_image,
            extra_container_config={"imagePullPolicy": "IfNotPresent"},
        )
    )


@pytest.fixture
def user_env():
    """The env var USER is not always set on non-linux systems."""
    if "USER" not in os.environ:
        os.environ["USER"] = getpass.getuser()
        yield
        del os.environ["USER"]
    else:
        yield


cluster_kwargs = {"asynchronous": True}


@pytest_asyncio.fixture
async def cluster(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, **cluster_kwargs) as cluster:
        yield cluster


@pytest_asyncio.fixture
async def remote_cluster(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, deploy_mode="remote", **cluster_kwargs) as cluster:
        yield cluster


@pytest_asyncio.fixture
async def client(cluster):
    async with Client(cluster, asynchronous=True) as client:
        yield client


@pytest.mark.asyncio
async def test_fixtures(client):
    """An initial test to get all the fixtures to run and check the cluster is usable."""
    assert client


@pytest.mark.asyncio
async def test_versions(client):
    await client.get_versions(check=True)


@pytest.mark.asyncio
async def test_cluster_create(cluster):
    cluster.scale(1)
    await cluster
    async with Client(cluster, asynchronous=True) as client:
        result = await client.submit(lambda x: x + 1, 10)
        assert result == 11


@pytest.mark.asyncio
async def test_basic(cluster, client):
    cluster.scale(2)
    future = client.submit(lambda x: x + 1, 10)
    result = await future
    assert result == 11

    await client.wait_for_workers(2)

    # Ensure that inter-worker communication works well
    futures = client.map(lambda x: x + 1, range(10))
    total = client.submit(sum, futures)
    assert (await total) == sum(map(lambda x: x + 1, range(10)))
    assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_logs(remote_cluster):
    cluster = remote_cluster
    cluster.scale(2)
    await cluster

    async with Client(cluster, asynchronous=True) as client:
        await client.wait_for_workers(2)

    logs = await cluster.get_logs()
    assert len(logs) == 4
    for _, log in logs.items():
        assert (
            "distributed.scheduler" in log
            or "distributed.worker" in log
            or "Creating scheduler pod" in log
        )


@pytest.mark.asyncio
async def test_dask_worker_name_env_variable(k8s_cluster, pod_spec, user_env):
    with dask.config.set({"kubernetes.name": "foo-{USER}-{uuid}"}):
        async with KubeCluster(pod_spec, **cluster_kwargs) as cluster:
            assert "foo-" + getpass.getuser() in cluster.name


@pytest.mark.asyncio
async def test_diagnostics_link_env_variable(k8s_cluster, pod_spec, user_env):
    pytest.importorskip("bokeh")
    with dask.config.set({"distributed.dashboard.link": "foo-{USER}-{port}"}):
        async with KubeCluster(pod_spec, asynchronous=True) as cluster:
            port = cluster.forwarded_dashboard_port

            assert (
                "foo-" + getpass.getuser() + "-" + str(port) in cluster.dashboard_link
            )


@pytest.mark.skip(reason="Cannot run two closers locally as loadbalancer ports collide")
@pytest.mark.asyncio
async def test_namespace(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, **cluster_kwargs) as cluster:
        assert "dask" in cluster.name
        assert getpass.getuser() in cluster.name
        async with KubeCluster(pod_spec, **cluster_kwargs) as cluster2:
            assert cluster.name != cluster2.name

            cluster2.scale(1)
            while len(await cluster2.pods()) != 1:
                await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_adapt(cluster):
    cluster.adapt()
    async with Client(cluster, asynchronous=True) as client:
        future = client.submit(lambda x: x + 1, 10)
        result = await future
        assert result == 11


@pytest.mark.xfail(reason="The widget has changed upstream")
@pytest.mark.asyncio
async def test_ipython_display(cluster):
    ipywidgets = pytest.importorskip("ipywidgets")
    cluster.scale(1)
    await cluster
    cluster._ipython_display_()
    box = cluster._cached_widget
    assert isinstance(box, ipywidgets.Widget)
    cluster._ipython_display_()
    assert cluster._cached_widget is box

    start = time()
    while "<td>1</td>" not in str(box):  # one worker in a table
        assert time() < start + 20
        await asyncio.sleep(0.5)


@pytest.mark.asyncio
async def test_env(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, env={"ABC": "DEF"}, **cluster_kwargs) as cluster:
        cluster.scale(1)
        await cluster
        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(1)
            env = await client.run(lambda: dict(os.environ))
            assert all(v["ABC"] == "DEF" for v in env.values())


@pytest.mark.asyncio
async def test_pod_from_yaml(k8s_cluster, docker_image):
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
        async with KubeCluster(f.name, **cluster_kwargs) as cluster:
            cluster.scale(2)
            await cluster
            async with Client(cluster, asynchronous=True) as client:
                future = client.submit(lambda x: x + 1, 10)
                result = await future.result(timeout=30)
                assert result == 11

                await client.wait_for_workers(2)

                # Ensure that inter-worker communication works well
                futures = client.map(lambda x: x + 1, range(10))
                total = client.submit(sum, futures)
                assert (await total) == sum(map(lambda x: x + 1, range(10)))
                assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_pod_expand_env_vars(k8s_cluster, docker_image):
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
            async with KubeCluster(f.name, **cluster_kwargs) as cluster:
                assert cluster.pod_template.spec.containers[0].image == docker_image
    finally:
        del os.environ["FOO_IMAGE"]


@pytest.mark.asyncio
async def test_pod_template_dict(docker_image):
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

    async with KubeCluster(spec, port=32000, **cluster_kwargs) as cluster:
        cluster.scale(2)
        await cluster
        async with Client(cluster, asynchronous=True) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = await future
            assert result == 11

            await client.wait_for_workers(2)

            # Ensure that inter-worker communication works well
            futures = client.map(lambda x: x + 1, range(10))
            total = client.submit(sum, futures)
            assert (await total) == sum(map(lambda x: x + 1, range(10)))
            assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_pod_template_minimal_dict(k8s_cluster, docker_image):
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

    async with KubeCluster(spec, **cluster_kwargs) as cluster:
        cluster.adapt()
        async with Client(cluster, asynchronous=True) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = await future
            assert result == 11


@pytest.mark.asyncio
async def test_pod_template_from_conf(docker_image):
    spec = {
        "spec": {
            "containers": [{"name": KUBECLUSTER_CONTAINER_NAME, "image": docker_image}]
        }
    }

    with dask.config.set({"kubernetes.worker-template": spec}):
        async with KubeCluster(**cluster_kwargs) as cluster:
            assert (
                cluster.pod_template.spec.containers[0].name
                == KUBECLUSTER_CONTAINER_NAME
            )


@pytest.mark.asyncio
async def test_pod_template_with_custom_container_name(docker_image):
    container_name = "my-custom-container"
    spec = {"spec": {"containers": [{"name": container_name, "image": docker_image}]}}

    with dask.config.set({"kubernetes.worker-template": spec}):
        async with KubeCluster(**cluster_kwargs) as cluster:
            assert cluster.pod_template.spec.containers[0].name == container_name


@pytest.mark.asyncio
async def test_constructor_parameters(k8s_cluster, pod_spec):
    env = {"FOO": "BAR", "A": 1}
    async with KubeCluster(
        pod_spec, name="myname", env=env, **cluster_kwargs
    ) as cluster:
        pod = cluster.pod_template
        assert pod.metadata.namespace == get_current_namespace()

        var = [v for v in pod.spec.containers[0].env if v.name == "FOO"]
        assert var and var[0].value == "BAR"

        var = [v for v in pod.spec.containers[0].env if v.name == "A"]
        assert var and var[0].value == "1"

        assert pod.metadata.generate_name == "myname"


@pytest.mark.asyncio
async def test_reject_evicted_workers(cluster):
    cluster.scale(1)
    await cluster

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        await asyncio.sleep(0.1)
        assert time() < start + 60

    # Evict worker
    [worker] = cluster.workers.values()
    await cluster.core_api.create_namespaced_pod_eviction(
        (await worker.describe_pod()).metadata.name,
        (await worker.describe_pod()).metadata.namespace,
        kubernetes.client.V1Eviction(
            delete_options=kubernetes.client.V1DeleteOptions(grace_period_seconds=300),
            metadata=(await worker.describe_pod()).metadata,
        ),
    )

    # Wait until worker removal has been picked up by scheduler
    start = time()
    while len(cluster.scheduler_info["workers"]) != 0:
        delta = time() - start
        assert delta < 60, f"Scheduler failed to remove worker in {delta:.0f}s"
        await asyncio.sleep(0.1)

    # Wait until worker removal has been handled by cluster
    while len(cluster.workers) != 0:
        delta = time() - start
        assert delta < 60, f"Cluster failed to remove worker in {delta:.0f}s"
        await asyncio.sleep(0.1)


@pytest.mark.asyncio
async def test_scale_up_down(cluster, client):
    np = pytest.importorskip("numpy")
    cluster.scale(2)
    await cluster

    start = time()
    while len(cluster.scheduler_info["workers"]) != 2:
        await asyncio.sleep(0.1)
        assert time() < start + 60

    a, b = list(cluster.scheduler_info["workers"])
    x = client.submit(np.ones, 1, workers=a)
    y = client.submit(np.ones, 50_000, workers=b)

    await wait([x, y])

    cluster.scale(1)
    await cluster

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        await asyncio.sleep(0.1)
        assert time() < start + 60

    # assert set(cluster.scheduler_info["workers"]) == {b}


@pytest.mark.xfail(
    reason="The delay between scaling up, starting a worker, and then scale down causes issues"
)
@pytest.mark.asyncio
async def test_scale_up_down_fast(cluster, client):
    cluster.scale(1)
    await cluster

    start = time()
    await client.wait_for_workers(1)

    worker = next(iter(cluster.scheduler_info["workers"].values()))

    # Put some data on this worker
    future = client.submit(lambda: b"\x00" * int(1e6))
    await wait(future)
    assert worker in cluster.scheduler.tasks[future.key].who_has

    # Rescale the cluster many times without waiting: this should put some
    # pressure on kubernetes but this should never fail nor delete our worker
    # with the temporary result.
    for i in range(10):
        await cluster._scale_up(4)
        await asyncio.sleep(random.random() / 2)
        cluster.scale(1)
        await asyncio.sleep(random.random() / 2)

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        await asyncio.sleep(0.1)
        assert time() < start + 20

    # The original task result is still stored on the original worker: this pod
    # has never been deleted when rescaling the cluster and the result can
    # still be fetched back.
    assert worker in cluster.scheduler.tasks[future.key].who_has
    assert len(await future) == int(1e6)


@pytest.mark.xfail(reason="scaling has some unfortunate state")
@pytest.mark.asyncio
async def test_scale_down_pending(cluster, client, cleanup_namespaces):
    # Try to scale the cluster to use more pods than available
    nodes = (await cluster.core_api.list_node()).items
    max_pods = sum(int(node.status.allocatable["pods"]) for node in nodes)
    if max_pods > 50:
        # It's probably not reasonable to run this test against a large
        # kubernetes cluster.
        pytest.skip("Require a small test kubernetes cluster (maxpod <= 50)")
    extra_pods = 5
    requested_pods = max_pods + extra_pods
    cluster.scale(requested_pods)

    start = time()
    while len(cluster.scheduler_info["workers"]) < 2:
        await asyncio.sleep(0.1)
        # Wait a bit because the kubernetes cluster can take time to provision
        # the requested pods as we requested a large number of pods.
        assert time() < start + 60

    pending_pods = [p for p in (await cluster.pods()) if p.status.phase == "Pending"]
    assert len(pending_pods) >= extra_pods

    running_workers = list(cluster.scheduler_info["workers"].keys())
    assert len(running_workers) >= 2

    # Put some data on those workers to make them important to keep as long
    # as possible.
    def load_data(i):
        return b"\x00" * (i * int(1e6))

    futures = [
        client.submit(load_data, i, workers=w) for i, w in enumerate(running_workers)
    ]
    await wait(futures)

    # Reduce the cluster size down to the actually useful nodes: pending pods
    # and running pods without results should be shutdown and removed first:
    cluster.scale(len(running_workers))

    start = time()
    pod_statuses = [p.status.phase for p in await cluster.pods()]
    while len(pod_statuses) != len(running_workers):
        if time() - start > 60:
            raise AssertionError(
                "Expected %d running pods but got %r"
                % (len(running_workers), pod_statuses)
            )
        await asyncio.sleep(0.1)
        pod_statuses = [p.status.phase for p in await cluster.pods()]

    assert pod_statuses == ["Running"] * len(running_workers)
    assert list(cluster.scheduler_info["workers"].keys()) == running_workers

    # Terminate everything
    cluster.scale(0)

    start = time()
    while len(cluster.scheduler_info["workers"]) > 0:
        await asyncio.sleep(0.1)
        assert time() < start + 60


@pytest.mark.asyncio
async def test_automatic_startup(k8s_cluster, docker_image):
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
            async with KubeCluster(**cluster_kwargs) as cluster:
                assert cluster.pod_template.metadata.labels["foo"] == "bar"


@pytest.mark.asyncio
async def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert "Box" not in text
        assert (
            cluster.scheduler.address in text
            or cluster.scheduler.external_address in text
        )


@pytest.mark.asyncio
async def test_escape_username(k8s_cluster, pod_spec, monkeypatch):
    monkeypatch.setenv("LOGNAME", "Foo!._")

    async with KubeCluster(pod_spec, **cluster_kwargs) as cluster:
        assert "foo" in cluster.name
        assert "!" not in cluster.name
        assert "." not in cluster.name
        assert "_" not in cluster.name
        assert "foo" in cluster.pod_template.metadata.labels["user"]


@pytest.mark.asyncio
async def test_escape_name(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, name="foo@bar", **cluster_kwargs) as cluster:
        assert "@" not in str(cluster.pod_template)


@pytest.mark.asyncio
async def test_maximum(cluster):
    with dask.config.set({"kubernetes.count.max": 1}):
        with captured_logger("dask_kubernetes") as logger:
            cluster.scale(10)
            await cluster

            start = time()
            while len(cluster.scheduler_info["workers"]) <= 0:
                await asyncio.sleep(0.1)
                assert time() < start + 60
            await asyncio.sleep(0.5)
            while len(cluster.scheduler_info["workers"]) != 1:
                await asyncio.sleep(0.1)
                assert time() < start + 60

        result = logger.getvalue()
        assert "scale beyond maximum number of workers" in result.lower()


def test_default_toleration(pod_spec):
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


def test_default_toleration_preserved(docker_image):
    pod_spec = clean_pod_template(
        make_pod_spec(
            image=docker_image,
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


@pytest.mark.asyncio
async def test_auth_missing(k8s_cluster, pod_spec):
    with pytest.raises(kubernetes.config.ConfigException) as info:
        await KubeCluster(pod_spec, auth=[], **cluster_kwargs)

    assert "No authorization methods were provided" in str(info.value)


@pytest.mark.asyncio
async def test_auth_tries_all_methods(k8s_cluster, pod_spec):
    fails = {"count": 0}

    class FailAuth(ClusterAuth):
        def load(self):
            fails["count"] += 1
            raise kubernetes.config.ConfigException("Fail #{count}".format(**fails))

    with pytest.raises(kubernetes.config.ConfigException) as info:
        await KubeCluster(pod_spec, auth=[FailAuth()] * 3, **cluster_kwargs)

    assert "Fail #3" in str(info.value)
    assert fails["count"] == 3


@pytest.mark.xfail(
    reason="Updating the default client configuration is broken in kubernetes"
)
@pytest.mark.asyncio
async def test_auth_kubeconfig_with_filename():
    await KubeConfig(config_file=CONFIG_DEMO).load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://1.2.3.4"
    assert config.cert_file == FAKE_CERT
    assert config.key_file == FAKE_KEY
    assert config.ssl_ca_cert == FAKE_CA


@pytest.mark.xfail(
    reason="Updating the default client configuration is broken in kubernetes"
)
@pytest.mark.asyncio
async def test_auth_kubeconfig_with_context():
    await KubeConfig(config_file=CONFIG_DEMO, context="exp-scratch").load()

    # we've set the default configuration, so check that it is default
    config = kubernetes.client.Configuration()
    assert config.host == "https://5.6.7.8"
    assert config.api_key["authorization"] == "Basic {}".format(
        base64.b64encode(b"exp:some-password").decode("ascii")
    )


@pytest.mark.xfail(
    reason="Updating the default client configuration is broken in async kubernetes"
)
@pytest.mark.asyncio
async def test_auth_explicit():
    await KubeAuth(
        host="https://9.8.7.6", username="abc", password="some-password"
    ).load()

    config = kubernetes.client.Configuration()
    assert config.host == "https://9.8.7.6"
    assert config.username == "abc"
    assert config.password == "some-password"
    assert config.get_basic_auth_token() == "Basic {}".format(
        base64.b64encode(b"abc:some-password").decode("ascii")
    )


@pytest.mark.asyncio
async def test_start_with_workers(k8s_cluster, pod_spec):
    async with KubeCluster(pod_spec, n_workers=2, **cluster_kwargs) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(2)


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Flaky in CI and classic is deprecated anyway")
async def test_adapt_delete(cluster, ns):
    """
    testing whether KubeCluster.adapt will bring
    back deleted worker pod (issue #244)
    """
    core_api = cluster.core_api

    async def get_worker_pods():
        pods_list = await core_api.list_namespaced_pod(
            namespace=ns,
            label_selector=f"dask.org/component=worker,dask.org/cluster-name={cluster.name}",
        )
        return [x.metadata.name for x in pods_list.items]

    cluster.adapt(maximum=2, minimum=2)
    start = time()
    while len(cluster.scheduler_info["workers"]) != 2:
        await asyncio.sleep(0.1)
        assert time() < start + 60

    worker_pods = await get_worker_pods()
    assert len(worker_pods) == 2
    # delete one worker pod
    to_delete = worker_pods[0]
    await core_api.delete_namespaced_pod(name=to_delete, namespace=ns)
    # wait until it is deleted
    start = time()
    while True:
        worker_pods = await get_worker_pods()
        if to_delete not in worker_pods:
            break
        await asyncio.sleep(0.1)
        assert time() < start + 60
    # test whether adapt will bring it back
    start = time()
    while len(cluster.scheduler_info["workers"]) != 2:
        await asyncio.sleep(0.1)
        assert time() < start + 60
    assert len(cluster.scheduler_info["workers"]) == 2


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Failing in CI with FileNotFoundError")
async def test_auto_refresh(cluster):
    config = {
        "apiVersion": "v1",
        "clusters": [
            {
                "cluster": {"certificate-authority-data": "", "server": ""},
                "name": "mock_gcp_config",
            }
        ],
        "contexts": [
            {
                "context": {
                    "cluster": "mock_gcp_config",
                    "user": "mock_gcp_config",
                },
                "name": "mock_gcp_config",
            }
        ],
        "current-context": "mock_gcp_config",
        "kind": "config",
        "preferences": {},
        "users": [
            {
                "name": "mock_gcp_config",
                "user": {
                    "auth-provider": {
                        "config": {
                            "access-token": "",
                            "cmd-args": "--fake-arg arg",
                            "cmd-path": f"{sys.executable} {TEST_DIR}/fake_gcp_auth.py",
                            "expiry": "",
                            "expiry-key": "{.credential.token_expiry}",
                            "toekn-key": "{.credential.access_token}",
                        },
                        "name": "gcp",
                    }
                },
            }
        ],
    }
    config_persister = False

    loader = dask_kubernetes.AutoRefreshKubeConfigLoader(
        config_dict=config,
        config_base_path=None,
        config_persister=config_persister,
    )

    await loader.load_gcp_token()
    # Check that we get back a token
    assert loader.token == f"Bearer {'0' * 137}"

    next_expire = loader.token_expire_ts
    for task in asyncio.all_tasks():
        if task.get_name() == "dask_auth_auto_refresh":
            await asyncio.wait_for(task, 10)

    # Ensure that our token expiration timer was refreshed
    assert loader.token_expire_ts > next_expire

    # Ensure refresh task was re-created
    for task in asyncio.all_tasks():
        if task.get_name() == "dask_auth_auto_refresh":
            loader.auto_refresh = False
            await asyncio.wait_for(task, 60)
            break
    else:
        assert False
