import pytest

import asyncio
from contextlib import asynccontextmanager
import pathlib
import os.path

from kopf.testing import KopfRunner

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture()
<<<<<<< HEAD
async def kopf_runner(k8s_cluster):
    yield KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"])
=======
async def operator(k8s_cluster):
    with KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"]) as runner:
        yield runner

    # Check operator completed successfully
    assert runner.exit_code == 0
    assert runner.exception is None
>>>>>>> Revert "Add tests for creating scheduler pod and service"


@pytest.fixture()
async def gen_cluster(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple cluster."""

<<<<<<< HEAD
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
=======
    # Delete cluster resource
    k8s_cluster.kubectl("delete", "-f", cluster_path)
    while cluster_name in k8s_cluster.kubectl("get", "daskclusters"):
        await asyncio.sleep(1)
>>>>>>> Revert "Add tests for creating scheduler pod and service"

    yield cm


def test_customresources(k8s_cluster):
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskworkergroups.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


def test_operator_runs(kopf_runner):
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None


@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_simplecluster(kopf_runner, gen_cluster):
    with kopf_runner as runner:
        async with gen_cluster() as cluster_name:
            # TODO test our cluster here
            assert cluster_name

    assert "A DaskCluster has been created" in runner.stdout
    # TODO test that the cluster has been cleaned up