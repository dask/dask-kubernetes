import pytest

import asyncio
from contextlib import asynccontextmanager
import pathlib

import os.path

from dask.distributed import Client

DIR = pathlib.Path(__file__).parent.absolute()


@pytest.fixture()
def gen_cluster(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple cluster."""

    @asynccontextmanager
    async def cm():
        cluster_path = os.path.join(DIR, "resources", "simplecluster.yaml")
        cluster_name = "simple"

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", cluster_path)
        while cluster_name not in k8s_cluster.kubectl("get", "daskclusters"):
            await asyncio.sleep(0.1)

        try:
            yield cluster_name
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-f", cluster_path)
            while cluster_name in k8s_cluster.kubectl("get", "daskclusters"):
                await asyncio.sleep(0.1)

    yield cm


@pytest.fixture()
def gen_job(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple job."""

    @asynccontextmanager
    async def cm():
        job_path = os.path.join(DIR, "resources", "simplejob.yaml")
        job_name = "simple-job"

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", job_path)
        while job_name not in k8s_cluster.kubectl("get", "daskjobs"):
            await asyncio.sleep(0.1)

        try:
            yield job_name
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-f", job_path)
            while job_name in k8s_cluster.kubectl("get", "daskjobs"):
                await asyncio.sleep(0.1)

    yield cm


def test_customresources(k8s_cluster):
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskworkergroups.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskjobs.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


def test_operator_runs(kopf_runner):
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None


def test_operator_plugins(kopf_runner):
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None
    assert "Plugin 'noop' running." in runner.stdout


@pytest.mark.asyncio
async def test_scalesimplecluster(k8s_cluster, kopf_runner, gen_cluster):
    with kopf_runner as runner:
        async with gen_cluster() as cluster_name:
            scheduler_pod_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"
            while scheduler_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc"):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)
            while "Running" not in k8s_cluster.kubectl(
                "get", "pods", scheduler_pod_name
            ):
                await asyncio.sleep(0.1)
            with k8s_cluster.port_forward(f"service/{service_name}", 8786) as port:
                async with Client(
                    f"tcp://localhost:{port}", asynchronous=True
                ) as client:
                    k8s_cluster.kubectl(
                        "scale",
                        "--replicas=5",
                        "daskworkergroup",
                        "simple-default",
                    )
                    await client.wait_for_workers(5)
                    k8s_cluster.kubectl(
                        "scale",
                        "--replicas=3",
                        "daskworkergroup",
                        "simple-default",
                    )
                    await client.wait_for_workers(3)


@pytest.mark.timeout(180)
@pytest.mark.asyncio
async def test_simplecluster(k8s_cluster, kopf_runner, gen_cluster):
    with kopf_runner as runner:
        async with gen_cluster() as cluster_name:
            scheduler_pod_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"

            while scheduler_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc"):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods"):
                await asyncio.sleep(0.1)

            with k8s_cluster.port_forward(f"service/{service_name}", 8786) as port:
                async with Client(
                    f"tcp://localhost:{port}", asynchronous=True
                ) as client:
                    await client.wait_for_workers(2)
                    # Ensure that inter-worker communication works well
                    futures = client.map(lambda x: x + 1, range(10))
                    total = client.submit(sum, futures)
                    assert (await total) == sum(map(lambda x: x + 1, range(10)))

            # Get the the first env value (the only one) of the scheduler
            scheduler_env = k8s_cluster.kubectl(
                "get",
                "pods",
                "--selector=dask.org/component=scheduler",
                "-o",
                "jsonpath='{.items[0].spec.containers[0].env[0]}'",
            )
            # Just check if its in the string, no need to parse the json
            assert "SCHEDULER_ENV" in scheduler_env

            # Get the the first env value (the only one) of the first worker
            worker_env = k8s_cluster.kubectl(
                "get",
                "pods",
                "--selector=dask.org/component=worker",
                "-o",
                "jsonpath='{.items[0].spec.containers[0].env[0]}'",
            )
            # Just check if its in the string, no need to parse the json
            assert "WORKER_ENV" in worker_env
            assert cluster_name


@pytest.mark.asyncio
async def test_job(k8s_cluster, kopf_runner, gen_job):
    with kopf_runner as runner:
        async with gen_job() as job:
            assert job

            runner_name = f"{job}-runner"

            # Assert that cluster is created
            while job not in k8s_cluster.kubectl("get", "daskclusters"):
                await asyncio.sleep(0.1)

            # Assert job pod is created
            while job not in k8s_cluster.kubectl("get", "po"):
                await asyncio.sleep(0.1)

            # Assert job pod runs to completion (will fail if doesn't connect to cluster)
            while "Completed" not in k8s_cluster.kubectl("get", "po", runner_name):
                await asyncio.sleep(0.1)

            # Assert cluster is removed on completion
            while job in k8s_cluster.kubectl("get", "daskclusters"):
                await asyncio.sleep(0.1)

    assert "A DaskJob has been created" in runner.stdout
    assert "Job succeeded, deleting Dask cluster." in runner.stdout
