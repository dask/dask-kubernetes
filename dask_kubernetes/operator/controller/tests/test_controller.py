import asyncio
import json
import os.path
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pytest
import yaml
from dask.distributed import Client

from dask_kubernetes.operator.controller import (
    KUBERNETES_DATETIME_FORMAT,
    get_job_runner_pod_name,
)

DIR = pathlib.Path(__file__).parent.absolute()


_EXPECTED_ANNOTATIONS = {"test-annotation": "annotation-value"}
_EXPECTED_LABELS = {"test-label": "label-value"}


@pytest.fixture()
def gen_cluster(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple cluster."""

    @asynccontextmanager
    async def cm():
        cluster_path = os.path.join(DIR, "resources", "simplecluster.yaml")
        cluster_name = "simple"

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", cluster_path)
        while cluster_name not in k8s_cluster.kubectl(
            "get", "daskclusters.kubernetes.dask.org"
        ):
            await asyncio.sleep(0.1)

        try:
            yield cluster_name
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-f", cluster_path)
            while cluster_name in k8s_cluster.kubectl(
                "get", "daskclusters.kubernetes.dask.org"
            ):
                await asyncio.sleep(0.1)

    yield cm


@pytest.fixture()
def gen_job(k8s_cluster):
    """Yields an instantiated context manager for creating/deleting a simple job."""

    @asynccontextmanager
    async def cm(job_file):
        job_path = os.path.join(DIR, "resources", job_file)
        with open(job_path) as f:
            job_name = yaml.load(f, yaml.Loader)["metadata"]["name"]

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-f", job_path)
        while job_name not in k8s_cluster.kubectl(
            "get", "daskjobs.kubernetes.dask.org"
        ):
            await asyncio.sleep(0.1)

        try:
            yield job_name
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-f", job_path)
            while job_name in k8s_cluster.kubectl(
                "get", "daskjobs.kubernetes.dask.org"
            ):
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
                        "daskworkergroup.kubernetes.dask.org",
                        "simple-default",
                    )
                    await client.wait_for_workers(5)
                    k8s_cluster.kubectl(
                        "scale",
                        "--replicas=3",
                        "daskworkergroup.kubernetes.dask.org",
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

            # Get the first env value (the only one) of the scheduler
            scheduler_env = k8s_cluster.kubectl(
                "get",
                "pods",
                "--selector=dask.org/component=scheduler",
                "-o",
                "jsonpath='{.items[0].spec.containers[0].env[0]}'",
            )
            # Just check if its in the string, no need to parse the json
            assert "SCHEDULER_ENV" in scheduler_env

            # Get the first annotation (the only one) of the scheduler
            scheduler_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "--selector=dask.org/component=scheduler",
                    "-o",
                    "jsonpath='{.items[0].metadata.annotations}'",
                )[1:-1]
            )  # First and last char is a quote
            assert _EXPECTED_ANNOTATIONS.items() <= scheduler_annotations.items()

            # Get the first annotation (the only one) of the scheduler
            service_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "svc",
                    "--selector=dask.org/cluster-name=simple",
                    "-o",
                    "jsonpath='{.items[0].metadata.annotations}'",
                )[1:-1]
            )  # First and last char is a quote
            assert _EXPECTED_ANNOTATIONS.items() <= service_annotations.items()

            # Get the first env value (the only one) of the first worker
            worker_env = k8s_cluster.kubectl(
                "get",
                "pods",
                "--selector=dask.org/component=worker",
                "-o",
                "jsonpath='{.items[0].spec.containers[0].env[0]}'",
            )
            # Just check if it's in the string, no need to parse the json
            assert "WORKER_ENV" in worker_env
            assert cluster_name

            # Get the first annotation (the only one) of a worker
            worker_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "--selector=dask.org/component=worker",
                    "-o",
                    "jsonpath='{.items[0].metadata.annotations}'",
                )[1:-1]
            )
            assert _EXPECTED_ANNOTATIONS.items() <= worker_annotations.items()

            # Assert labels from the dask cluster are propagated into the dask worker group
            workergroup_labels = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "daskworkergroups",
                    "--selector=dask.org/component=workergroup",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= workergroup_labels.items()

            # Assert labels from the dask cluster are propagated into the dask scheduler
            scheduler_labels = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "--selector=dask.org/component=scheduler",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= workergroup_labels.items()

            # Assert labels from the dask cluster are propagated into the dask worker pod
            worker_labels = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "--selector=dask.org/component=worker",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= workergroup_labels.items()


def _get_job_status(k8s_cluster):
    return json.loads(
        k8s_cluster.kubectl(
            "get",
            "daskjobs.kubernetes.dask.org",
            "-o",
            "jsonpath='{.items[0].status}'",
        )[1:-1]
    )


def _assert_job_status_created(job_status):
    assert "jobStatus" in job_status


def _assert_job_status_cluster_created(job, job_status):
    assert "jobStatus" in job_status
    assert job_status["clusterName"] == job
    assert job_status["jobRunnerPodName"] == get_job_runner_pod_name(job)


def _assert_job_status_running(job, job_status):
    assert "jobStatus" in job_status
    assert job_status["clusterName"] == job
    assert job_status["jobRunnerPodName"] == get_job_runner_pod_name(job)
    start_time = datetime.strptime(job_status["startTime"], KUBERNETES_DATETIME_FORMAT)
    assert datetime.utcnow() > start_time > (datetime.utcnow() - timedelta(seconds=10))


def _assert_final_job_status(job, job_status, expected_status):
    assert job_status["jobStatus"] == expected_status
    assert job_status["clusterName"] == job
    assert job_status["jobRunnerPodName"] == get_job_runner_pod_name(job)
    start_time = datetime.strptime(job_status["startTime"], KUBERNETES_DATETIME_FORMAT)
    assert datetime.utcnow() > start_time > (datetime.utcnow() - timedelta(minutes=1))
    end_time = datetime.strptime(job_status["endTime"], KUBERNETES_DATETIME_FORMAT)
    assert datetime.utcnow() > end_time > (datetime.utcnow() - timedelta(minutes=1))
    assert set(job_status.keys()) == {
        "clusterName",
        "jobRunnerPodName",
        "jobStatus",
        "startTime",
        "endTime",
    }


@pytest.mark.asyncio
async def test_job(k8s_cluster, kopf_runner, gen_job):
    with kopf_runner as runner:
        async with gen_job("simplejob.yaml") as job:
            assert job

            runner_name = f"{job}-runner"

            # Assert that job was created
            while job not in k8s_cluster.kubectl("get", "daskjobs.kubernetes.dask.org"):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_created(job_status)

            # Assert that cluster is created
            while job not in k8s_cluster.kubectl(
                "get", "daskclusters.kubernetes.dask.org"
            ):
                await asyncio.sleep(0.1)

            await asyncio.sleep(0.1)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_cluster_created(job, job_status)

            # Assert job pod is created
            while job not in k8s_cluster.kubectl("get", "po"):
                await asyncio.sleep(0.1)

            # Assert that pod started Running
            while "Running" not in k8s_cluster.kubectl("get", "po"):
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_running(job, job_status)

            job_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "--selector=dask.org/component=job-runner",
                    "-o",
                    "jsonpath='{.items[0].metadata.annotations}'",
                )[1:-1]
            )
            _EXPECTED_ANNOTATIONS.items() <= job_annotations.items()

            # Assert job pod runs to completion (will fail if doesn't connect to cluster)
            while "Completed" not in k8s_cluster.kubectl("get", "po", runner_name):
                await asyncio.sleep(0.1)

            # Assert cluster is removed on completion
            while job in k8s_cluster.kubectl("get", "daskclusters.kubernetes.dask.org"):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster)
            _assert_final_job_status(job, job_status, "Successful")

    assert "A DaskJob has been created" in runner.stdout
    assert "Job succeeded, deleting Dask cluster." in runner.stdout


@pytest.mark.asyncio
async def test_failed_job(k8s_cluster, kopf_runner, gen_job):
    with kopf_runner as runner:
        async with gen_job("failedjob.yaml") as job:
            assert job

            runner_name = f"{job}-runner"

            # Assert that job was created
            while job not in k8s_cluster.kubectl("get", "daskjobs.kubernetes.dask.org"):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_created(job_status)

            # Assert that cluster is created
            while job not in k8s_cluster.kubectl(
                "get", "daskclusters.kubernetes.dask.org"
            ):
                await asyncio.sleep(0.1)

            await asyncio.sleep(0.1)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_cluster_created(job, job_status)

            # Assert job pod is created
            while job not in k8s_cluster.kubectl("get", "po"):
                await asyncio.sleep(0.1)

            # Assert that pod started Running
            while "Running" not in k8s_cluster.kubectl("get", "po"):
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster)
            _assert_job_status_running(job, job_status)

            # Assert job pod runs to failure
            while "Error" not in k8s_cluster.kubectl("get", "po", runner_name):
                await asyncio.sleep(0.1)

            # Assert cluster is removed on completion
            while job in k8s_cluster.kubectl("get", "daskclusters.kubernetes.dask.org"):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster)
            _assert_final_job_status(job, job_status, "Failed")

    assert "A DaskJob has been created" in runner.stdout
    assert "Job failed, deleting Dask cluster." in runner.stdout
