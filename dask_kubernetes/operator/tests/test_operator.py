import pytest

import pathlib
import subprocess
import os
import time
from contextlib import suppress

from kopf.testing import KopfRunner

from dask.distributed import Client

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture(scope="session", autouse=True)
def crds(k8s_cluster):
    crd_path = os.path.join(DIR, "..", "crds.yaml")
    k8s_cluster.kubectl("apply", "-f", crd_path)
    yield
    k8s_cluster.kubectl("delete", "-f", crd_path)


def test_crds(k8s_cluster):
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


@pytest.mark.timeout(60)
@pytest.mark.asyncio
async def test_operator(k8s_cluster):
    cluster_path = os.path.join(DIR, "resources", "simplecluster.yaml")
    cluster_name = "simple-cluster"

    # Start operator
    with KopfRunner(["run", "-m", "dask_kubernetes.operator", "--verbose"]) as runner:

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", cluster_path)
        while cluster_name not in k8s_cluster.kubectl("get", "svc"):
            time.sleep(1)

        # Check connectivity
        with k8s_cluster.port_forward(f"service/{cluster_name}", 8786) as port:
            async with Client(f"tcp://localhost:{port}", asynchronous=True) as client:
                await client.wait_for_workers(2)
                # Ensure that inter-worker communication works well
                futures = client.map(lambda x: x + 1, range(10))
                total = client.submit(sum, futures)
                assert (await total) == sum(map(lambda x: x + 1, range(10)))
                assert all((await client.has_what()).values())

        # Delete cluster resource
        k8s_cluster.kubectl("delete", "-f", cluster_path)
        while cluster_name in k8s_cluster.kubectl("get", "svc"):
            time.sleep(1)

    # Check operator completed successfully
    assert runner.exit_code == 0
    assert runner.exception is None
    assert "Scaled cluster to 2 workers." in runner.stdout
