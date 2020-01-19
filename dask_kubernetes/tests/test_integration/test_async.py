import asyncio

import getpass
import os
import random
from time import time
import yaml

import kubernetes_asyncio as kubernetes
import pytest

import dask
from dask.distributed import Client, wait
from dask_kubernetes import (
    KubeCluster,
    make_pod_spec,
    clean_pod_template,
)
from distributed.utils import tmpfile
from distributed.utils_test import captured_logger


@pytest.fixture
def pod_spec(image_name):
    yield clean_pod_template(
        make_pod_spec(
            image=image_name, extra_container_config={"imagePullPolicy": "IfNotPresent"}
        )
    )


@pytest.fixture(scope="module")
def event_loop(request):
    """Override function-scoped fixture in pytest-asyncio."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


cluster_kwargs = {"asynchronous": True}


@pytest.fixture
async def cluster(pod_spec, ns, auth):
    async with KubeCluster(
        pod_spec, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        yield cluster


@pytest.fixture
async def remote_cluster(pod_spec, ns, auth):
    async with KubeCluster(
        pod_spec, namespace=ns, deploy_mode="remote", auth=auth, **cluster_kwargs
    ) as cluster:
        yield cluster


@pytest.fixture
async def client(cluster):
    async with Client(cluster, asynchronous=True) as client:
        yield client


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

    while len(cluster.scheduler_info["workers"]) < 2:
        await asyncio.sleep(0.1)

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

    start = time()
    while len(cluster.scheduler_info["workers"]) < 2:
        await asyncio.sleep(0.1)
        assert time() < start + 20

    logs = await cluster.logs()
    assert len(logs) == 3
    for _, log in logs.items():
        assert "distributed.scheduler" in log or "distributed.worker" in log


@pytest.mark.asyncio
async def test_diagnostics_link_env_variable(pod_spec, ns):
    pytest.importorskip("bokeh")
    with dask.config.set({"distributed.dashboard.link": "foo-{USER}-{port}"}):
        async with KubeCluster(pod_spec, namespace=ns, asynchronous=True) as cluster:
            port = cluster.scheduler_info["services"]["dashboard"]

            assert (
                "foo-" + getpass.getuser() + "-" + str(port) in cluster.dashboard_link
            )


@pytest.mark.skip(reason="Cannot run two closers locally as loadbalancer ports collide")
@pytest.mark.asyncio
async def test_namespace(pod_spec, ns, auth):
    async with KubeCluster(
        pod_spec, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        assert "dask" in cluster.name
        assert getpass.getuser() in cluster.name
        async with KubeCluster(pod_spec, namespace=ns, **cluster_kwargs) as cluster2:
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

    start = time()
    while cluster.scheduler_info["workers"]:
        await asyncio.sleep(0.1)
        assert time() < start + 20


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
async def test_env(pod_spec, ns, auth):
    async with KubeCluster(
        pod_spec, env={"ABC": "DEF"}, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        cluster.scale(1)
        await cluster
        async with Client(cluster, asynchronous=True) as client:
            while not cluster.scheduler_info["workers"]:
                await asyncio.sleep(0.1)
            env = await client.run(lambda: dict(os.environ))
            assert all(v["ABC"] == "DEF" for v in env.values())


@pytest.mark.asyncio
async def test_pod_from_yaml(image_name, ns, auth):
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
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "dask-worker",
                }
            ]
        },
    }

    with tmpfile(extension="yaml") as fn:
        with open(fn, mode="w") as f:
            yaml.dump(test_yaml, f)
        async with KubeCluster.from_yaml(
            f.name, namespace=ns, auth=auth, **cluster_kwargs
        ) as cluster:
            assert cluster.namespace == ns
            cluster.scale(2)
            await cluster
            async with Client(cluster, asynchronous=True) as client:
                future = client.submit(lambda x: x + 1, 10)
                result = await future.result(timeout=10)
                assert result == 11

                start = time()
                while len(cluster.scheduler_info["workers"]) < 2:
                    await asyncio.sleep(0.1)
                    assert time() < start + 20, "timeout"

                # Ensure that inter-worker communication works well
                futures = client.map(lambda x: x + 1, range(10))
                total = client.submit(sum, futures)
                assert (await total) == sum(map(lambda x: x + 1, range(10)))
                assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_pod_from_dict(image_name, ns, auth):
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
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "dask-worker",
                }
            ]
        },
    }

    async with KubeCluster.from_dict(
        spec, namespace=ns, port=32000, auth=auth, **cluster_kwargs
    ) as cluster:
        cluster.scale(2)
        await cluster
        assert "32000" in cluster.scheduler_address
        async with Client(cluster, asynchronous=True) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = await future
            assert result == 11

            while len(cluster.scheduler_info["workers"]) < 2:
                await asyncio.sleep(0.1)

            # Ensure that inter-worker communication works well
            futures = client.map(lambda x: x + 1, range(10))
            total = client.submit(sum, futures)
            assert (await total) == sum(map(lambda x: x + 1, range(10)))
            assert all((await client.has_what()).values())


@pytest.mark.asyncio
async def test_pod_from_minimal_dict(image_name, ns, auth):
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
                    "image": image_name,
                    "imagePullPolicy": "IfNotPresent",
                    "name": "worker",
                }
            ]
        }
    }

    async with KubeCluster.from_dict(
        spec, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        cluster.adapt()
        async with Client(cluster, asynchronous=True) as client:
            future = client.submit(lambda x: x + 1, 10)
            result = await future
            assert result == 11


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
        kubernetes.client.V1beta1Eviction(
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
        assert time() < start + 20

    a, b = list(cluster.scheduler_info["workers"])
    x = client.submit(np.ones, 1, workers=a)
    y = client.submit(np.ones, 50_000, workers=b)

    await wait([x, y])

    cluster.scale(1)
    await cluster

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        await asyncio.sleep(0.1)
        assert time() < start + 20

    # assert set(cluster.scheduler_info["workers"]) == {b}


@pytest.mark.asyncio
async def test_scale_up_down_fast(cluster, client):
    cluster.scale(1)
    await cluster

    start = time()
    while len(cluster.scheduler_info["workers"]) != 1:
        await asyncio.sleep(0.1)
        assert time() < start + 20

    worker = next(iter(cluster.scheduler_info["workers"].values()))

    # Put some data on this worker
    future = client.submit(lambda: b"\x00" * int(1e6))
    await wait(future)
    havers_names = {ws.name for ws in cluster.scheduler.tasks[future.key].who_has}
    assert worker["name"] in havers_names

    # Rescale the cluster many times without waiting: this should put some
    # pressure on kubernetes but this should never fail nor delete our worker
    # with the temporary result.
    for i in range(10):
        cluster.scale(4)
        await cluster
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
    havers_names = {ws.name for ws in cluster.scheduler.tasks[future.key].who_has}
    assert worker["name"] in havers_names
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
async def test_repr(cluster):
    for text in [repr(cluster), str(cluster)]:
        assert "Box" not in text
        assert (
            cluster.scheduler.address in text
            or cluster.scheduler.external_address in text
        )


@pytest.mark.asyncio
async def test_escape_username(pod_spec, ns, auth, monkeypatch):
    monkeypatch.setenv("LOGNAME", "foo!")

    async with KubeCluster(
        pod_spec, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        assert "foo" in cluster.name
        assert "!" not in cluster.name
        assert "foo" in cluster.rendered_worker_pod_template.metadata.labels["user"]


@pytest.mark.asyncio
async def test_escape_name(pod_spec, auth, ns):
    async with KubeCluster(
        pod_spec, namespace=ns, name="foo@bar", auth=auth, **cluster_kwargs
    ) as cluster:
        assert "@" not in str(cluster.rendered_worker_pod_template)


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
            assert len(cluster.scheduler_info["workers"]) == 1

        result = logger.getvalue()
        assert "scale beyond maximum number of workers" in result.lower()


@pytest.mark.asyncio
async def test_start_with_workers(pod_spec, ns, auth):
    async with KubeCluster(
        pod_spec, n_workers=2, namespace=ns, auth=auth, **cluster_kwargs
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            while len(cluster.scheduler_info["workers"]) != 2:
                await asyncio.sleep(0.1)
