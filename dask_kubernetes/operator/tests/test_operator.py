import pytest

import asyncio
from contextlib import asynccontextmanager
import pathlib
import os.path

from kopf.testing import KopfRunner

from dask.distributed import Client

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture()
async def kopf_runner(k8s_cluster):
    yield KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"])


@pytest.fixture()
async def gen_cluster(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple cluster."""

    @asynccontextmanager
    async def cm():
        cluster_path = os.path.join(DIR, "resources", "simplecluster.yaml")
        cluster_name = "simple-cluster"

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", cluster_path)
        while cluster_name not in k8s_cluster.kubectl("get", "daskclusters"):
            await asyncio.sleep(0.1)

        try:
            yield cluster_name
        finally:
            # Delete cluster resource
            k8s_cluster.kubectl("delete", "-f", cluster_path)
            while cluster_name in k8s_cluster.kubectl("get", "daskclusters"):
                await asyncio.sleep(0.1)

    yield cm


def test_customresources(k8s_cluster, gen_cluster):
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskworkergroups.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


def test_operator_runs(kopf_runner):
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None


@pytest.mark.timeout(180)
@pytest.mark.asyncio
async def test_simplecluster(k8s_cluster, kopf_runner, gen_cluster):
    with kopf_runner as runner:
        async with gen_cluster() as cluster_name:
            scheduler_pod_name = "simple-cluster-scheduler"
            worker_pod_name = "simple-cluster-worker-1"
            while scheduler_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)
            while "Running" not in k8s_cluster.kubectl(
                "get", "pods", scheduler_pod_name
            ):
                await asyncio.sleep(0.1)
            while cluster_name not in k8s_cluster.kubectl("get", "svc"):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)
            with k8s_cluster.port_forward(f"service/{cluster_name}", 8786) as port:
                async with Client(
                    f"{cluster_name}:{port}", asynchronous=True
                ) as client:
                    await client.wait_for_workers(2)
                    # Ensure that inter-worker communication works well
                    futures = client.map(lambda x: x + 1, range(10))
                    total = client.submit(sum, futures)
                    assert (await total) == sum(map(lambda x: x + 1, range(10)))
            assert cluster_name

    assert "A DaskCluster has been created" in runner.stdout
    assert "A scheduler pod has been created" in runner.stdout
    assert "A worker group has been created" in runner.stdout
    # TODO test that the cluster has been cleaned up
