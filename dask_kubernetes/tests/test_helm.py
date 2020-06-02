import pytest

import subprocess
import os.path
import time


@pytest.fixture(scope="session")
def release_name():
    return "testdask"


@pytest.fixture(scope="session")
def config_path():
    return os.path.join(os.path.dirname(__file__), "helm", "values.yaml")


@pytest.fixture(scope="session")  # Creating this fixture is slow so we should reuse it.
def release(release_name, config_path):
    subprocess.run(
        ["helm", "install", release_name, "dask/dask", "--wait", "-f", config_path],
        capture_output=True,
        encoding="utf-8",
    )
    time.sleep(5)  # Wait for scheduler to start. TODO Replace with more robust check.
    yield release_name
    subprocess.run(
        ["helm", "delete", release_name], capture_output=True, encoding="utf-8"
    )


@pytest.fixture
async def cluster(release):
    from dask_kubernetes import HelmCluster

    async with HelmCluster(release_name=release, asynchronous=True) as cluster:
        await cluster
        yield cluster


@pytest.fixture
def sync_cluster(release):
    from dask_kubernetes import HelmCluster

    with HelmCluster(release_name=release, asynchronous=False) as cluster:
        yield cluster


def test_import():
    from dask_kubernetes import HelmCluster


def test_raises_on_non_existant_release():
    from dask_kubernetes import HelmCluster

    with pytest.raises(RuntimeError):
        HelmCluster(release_name="nosuchrelease", namespace="default")


@pytest.mark.asyncio
async def test_create_helm_cluster(cluster, release_name):
    assert cluster.status == "running"
    assert cluster.release_name == release_name
    assert "id" in cluster.scheduler_info


def test_create_sync_helm_cluster(sync_cluster, release_name):
    cluster = sync_cluster
    assert cluster.status == "running"
    assert cluster.release_name == release_name
    assert "id" in cluster.scheduler_info


@pytest.mark.asyncio
async def test_scale_cluster(cluster):
    # Scale up
    await cluster.scale(4)
    await cluster  # Wait for workers
    assert len(cluster.scheduler_info["workers"]) == 4

    # Scale down
    await cluster.scale(3)
    await cluster  # Wait for workers
    assert len(cluster.scheduler_info["workers"]) == 3


@pytest.mark.asyncio
async def test_logs(cluster):
    from distributed.utils import Logs

    logs = await cluster.logs()

    assert isinstance(logs, Logs)
    assert any(["scheduler" in log for log in logs])
    assert any(["worker" in log for log in logs])

    [scheduler_logs] = [logs[log] for log in logs if "scheduler" in log]
    assert "Scheduler at:" in scheduler_logs


@pytest.mark.asyncio
async def test_adaptivity_warning(cluster):
    with pytest.warns(UserWarning):
        await cluster.adapt(minimum=3, maximum=3)
