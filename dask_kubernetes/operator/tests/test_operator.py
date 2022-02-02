import pytest

import asyncio
import pathlib
import os.path

from kopf.testing import KopfRunner

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture()
async def operator(k8s_cluster):
    with KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"]) as runner:
        yield runner

    # Check operator completed successfully
    assert runner.exit_code == 0
    assert runner.exception is None


@pytest.fixture()
async def simplecluster(k8s_cluster, operator):
    cluster_path = os.path.join(DIR, "resources", "simplecluster.yaml")
    cluster_name = "simple-cluster"

    # Create cluster resource
    k8s_cluster.kubectl("apply", "-f", cluster_path)
    while cluster_name not in k8s_cluster.kubectl("get", "daskclusters"):
        await asyncio.sleep(1)

    yield cluster_name

    # Delete cluster resource
    k8s_cluster.kubectl("delete", "-f", cluster_path)
    while cluster_name in k8s_cluster.kubectl("get", "daskclusters"):
        await asyncio.sleep(1)

    # FIXME stdout may not have triggered or flushed by this point but we should check that this was successful
    # assert "A DaskCluster has been created" in operator.stdout


def test_customresources(k8s_cluster):
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskworkergroups.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_simplecluster(simplecluster):
    # If we get to this point then all fixtures worked ok and we can actually test some things
    assert simplecluster
