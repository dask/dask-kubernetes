import pytest

import subprocess
import os.path
import time

from distributed.core import Status

###############
# Fixtures
##


@pytest.fixture(scope="session")
def chart_repo():
    repo_name = "dask"
    subprocess.check_output(
        ["helm", "repo", "add", repo_name, "https://helm.dask.org/"]
    )
    subprocess.check_output(["helm", "repo", "update"])
    return repo_name


@pytest.fixture(scope="session")
def chart_name(chart_repo):
    chart = "dask"
    return f"{chart_repo}/{chart}"


@pytest.fixture(scope="session")
def config_path():
    return os.path.join(os.path.dirname(__file__), "helm", "values.yaml")


@pytest.fixture(scope="session")
def release_name():
    return "testrelease"


@pytest.fixture(scope="session")  # Creating this fixture is slow so we should reuse it.
def release(k8s_cluster, chart_name, release_name, config_path):
    subprocess.check_output(
        ["helm", "install", release_name, chart_name, "--wait", "-f", config_path]
    )
    # time.sleep(10)  # Wait for scheduler to start. TODO Replace with more robust check.
    yield release_name
    subprocess.check_output(["helm", "delete", release_name])


@pytest.fixture
async def cluster(k8s_cluster, release):
    from dask_kubernetes import HelmCluster

    async with HelmCluster(release_name=release, asynchronous=True) as cluster:
        await cluster
        yield cluster


@pytest.fixture
def sync_cluster(k8s_cluster, release):
    from dask_kubernetes import HelmCluster

    with HelmCluster(release_name=release, asynchronous=False) as cluster:
        yield cluster


###############
# Tests
##


def test_import():
    from dask_kubernetes import HelmCluster
    from distributed.deploy import Cluster

    assert issubclass(HelmCluster, Cluster)


def test_raises_on_non_existant_release(k8s_cluster):
    from dask_kubernetes import HelmCluster

    with pytest.raises(RuntimeError):
        HelmCluster(release_name="nosuchrelease", namespace="default")


@pytest.mark.asyncio
async def test_create_helm_cluster(cluster, release_name):
    assert cluster.status == Status.running
    assert cluster.release_name == release_name
    assert "id" in cluster.scheduler_info


def test_create_sync_helm_cluster(sync_cluster, release_name):
    cluster = sync_cluster
    assert cluster.status == Status.running
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

    logs = await cluster.get_logs()

    assert isinstance(logs, Logs)
    assert any(["scheduler" in log for log in logs])
    assert any(["worker" in log for log in logs])

    [scheduler_logs] = [logs[log] for log in logs if "scheduler" in log]
    assert "Scheduler at:" in scheduler_logs


@pytest.mark.asyncio
async def test_adaptivity_warning(cluster):
    with pytest.raises(NotImplementedError):
        await cluster.adapt(minimum=3, maximum=3)
