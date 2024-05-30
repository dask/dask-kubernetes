from __future__ import annotations

import asyncio
import json
import os.path
import pathlib
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Final,
    Iterator,
)

import dask.config
import pytest
import yaml
from dask.distributed import Client
from kr8s.asyncio.objects import Deployment, Pod, Service

from dask_kubernetes.constants import MAX_CLUSTER_NAME_LEN
from dask_kubernetes.operator._objects import DaskCluster, DaskJob, DaskWorkerGroup
from dask_kubernetes.operator.controller import (
    KUBERNETES_DATETIME_FORMAT,
    get_job_runner_pod_name,
)

if TYPE_CHECKING:
    from kopf.testing import KopfRunner
    from pytest_kind.cluster import KindCluster

DIR: Final[pathlib.Path] = pathlib.Path(__file__).parent.absolute()

_EXPECTED_ANNOTATIONS: Final[dict[str, str]] = {"test-annotation": "annotation-value"}
_EXPECTED_LABELS: Final[dict[str, str]] = {"test-label": "label-value"}
DEFAULT_CLUSTER_NAME: Final[str] = "simple"


@pytest.fixture()
def gen_cluster_manifest(tmp_path: pathlib.Path) -> Callable[..., pathlib.Path]:
    def factory(cluster_name: str = DEFAULT_CLUSTER_NAME) -> pathlib.Path:
        original_manifest_path = os.path.join(DIR, "resources", "simplecluster.yaml")
        with open(original_manifest_path, "r") as original_manifest_file:
            manifest = yaml.safe_load(original_manifest_file)

        manifest["metadata"]["name"] = cluster_name
        new_manifest_path = tmp_path / "cluster.yaml"
        new_manifest_path.write_text(yaml.safe_dump(manifest))
        return tmp_path

    return factory


@pytest.fixture()
def gen_cluster(
    k8s_cluster: KindCluster,
    namespace: str,
    gen_cluster_manifest: Callable[..., pathlib.Path],
) -> Iterator[Callable[..., AsyncContextManager[tuple[str, str]]]]:
    """Yields an instantiated context manager for creating/deleting a simple cluster."""

    @asynccontextmanager
    async def cm(
        cluster_name: str = DEFAULT_CLUSTER_NAME,
    ) -> AsyncIterator[tuple[str, str]]:
        cluster_path = gen_cluster_manifest(cluster_name)
        # Create cluster resource
        k8s_cluster.kubectl("apply", "-n", namespace, "-f", str(cluster_path))
        while cluster_name not in k8s_cluster.kubectl(
            "get", "daskclusters.kubernetes.dask.org", "-n", namespace
        ):
            await asyncio.sleep(0.1)

        try:
            yield cluster_name, namespace
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-n", namespace, "-f", str(cluster_path))

    yield cm


@pytest.fixture()
def gen_job(
    k8s_cluster: KindCluster, namespace: str
) -> Iterator[Callable[[str], AsyncContextManager[tuple[str, str]]]]:
    """Yields an instantiated context manager for creating/deleting a simple job."""

    @asynccontextmanager
    async def cm(job_file: str) -> AsyncIterator[tuple[str, str]]:
        job_path = os.path.join(DIR, "resources", job_file)
        with open(job_path) as f:
            job_name = yaml.load(f, yaml.Loader)["metadata"]["name"]

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-n", namespace, "-f", job_path)
        while job_name not in k8s_cluster.kubectl(
            "get", "daskjobs.kubernetes.dask.org", "-n", namespace
        ):
            await asyncio.sleep(0.1)

        try:
            yield job_name, namespace
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-n", namespace, "-f", job_path)
            while job_name in k8s_cluster.kubectl(
                "get", "daskjobs.kubernetes.dask.org", "-n", namespace
            ):
                await asyncio.sleep(0.1)

    yield cm


@pytest.fixture()
def gen_worker_group(
    k8s_cluster: KindCluster, namespace: str
) -> Iterator[Callable[[str], AsyncContextManager[tuple[str, str]]]]:
    """Yields an instantiated context manager for creating/deleting a worker group."""

    @asynccontextmanager
    async def cm(worker_group_file: str) -> AsyncIterator[tuple[str, str]]:
        worker_group_path = os.path.join(DIR, "resources", worker_group_file)
        with open(worker_group_path) as f:
            worker_group_name = yaml.load(f, yaml.Loader)["metadata"]["name"]

        # Create cluster resource
        k8s_cluster.kubectl("apply", "-n", namespace, "-f", worker_group_path)
        while worker_group_name not in k8s_cluster.kubectl(
            "get", "daskworkergroups.kubernetes.dask.org", "-n", namespace
        ):
            await asyncio.sleep(0.1)

        try:
            yield worker_group_name, namespace
        finally:
            # Test: remove the wait=True, because I think this is blocking the operator
            k8s_cluster.kubectl("delete", "-n", namespace, "-f", worker_group_path)
            while worker_group_name in k8s_cluster.kubectl(
                "get", "daskworkergroups.kubernetes.dask.org", "-n", namespace
            ):
                await asyncio.sleep(0.1)

    yield cm


def test_customresources(k8s_cluster: KindCluster) -> None:
    assert "daskclusters.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskworkergroups.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")
    assert "daskjobs.kubernetes.dask.org" in k8s_cluster.kubectl("get", "crd")


def test_operator_runs(kopf_runner: KopfRunner) -> None:
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None


def test_operator_plugins(kopf_runner: KopfRunner) -> None:
    with kopf_runner as runner:
        pass

    assert runner.exit_code == 0
    assert runner.exception is None
    assert "Plugin 'noop' running." in runner.stdout


@pytest.mark.timeout(180)
@pytest.mark.anyio
async def test_simplecluster(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            scheduler_deployment_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"

            while scheduler_deployment_name not in k8s_cluster.kubectl(
                "get", "deployments", "-n", ns
            ):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc", "-n", ns):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods", "-n", ns):
                await asyncio.sleep(0.1)

            with k8s_cluster.port_forward(
                f"service/{service_name}", 8786, "-n", ns
            ) as port:
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
                "-n",
                ns,
                "--selector=dask.org/component=scheduler",
                "-o",
                "jsonpath='{.items[0].spec.containers[0].env[0]}'",
            )
            # Just check if it's in the string, no need to parse the json
            assert "SCHEDULER_ENV" in scheduler_env

            # Get the first annotation (the only one) of the scheduler
            scheduler_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "-n",
                    ns,
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
                    "-n",
                    ns,
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
                "-n",
                ns,
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
                    "-n",
                    ns,
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
                    "-n",
                    ns,
                    "--selector=dask.org/component=workergroup",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= workergroup_labels.items()
            assert "worker-sublabel" in workergroup_labels

            # Assert labels from the dask cluster are propagated into the dask scheduler
            scheduler_labels = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "-n",
                    ns,
                    "--selector=dask.org/component=scheduler",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= scheduler_labels.items()
            assert "scheduler-sublabel" in scheduler_labels

            # Assert labels from the dask cluster are propagated into the dask worker pod
            worker_labels = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "-n",
                    ns,
                    "--selector=dask.org/component=worker",
                    "-o",
                    "jsonpath='{.items[0].metadata.labels}'",
                )[1:-1]
            )
            assert _EXPECTED_LABELS.items() <= worker_labels.items()
            assert "worker-sublabel" in workergroup_labels


@pytest.mark.anyio
async def test_scalesimplecluster(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            scheduler_deployment_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"
            while scheduler_deployment_name not in k8s_cluster.kubectl(
                "get", "deployments", "-n", ns
            ):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc", "-n", ns):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods", "-n", ns):
                await asyncio.sleep(0.1)

            with k8s_cluster.port_forward(
                f"service/{service_name}", 8786, "-n", ns
            ) as port:
                async with Client(
                    f"tcp://localhost:{port}", asynchronous=True
                ) as client:
                    k8s_cluster.kubectl(
                        "scale",
                        "-n",
                        ns,
                        "--replicas=5",
                        "daskworkergroup.kubernetes.dask.org",
                        "simple-default",
                    )
                    await client.wait_for_workers(5)
                    k8s_cluster.kubectl(
                        "scale",
                        "-n",
                        ns,
                        "--replicas=3",
                        "daskworkergroup.kubernetes.dask.org",
                        "simple-default",
                    )
                    # TODO: Currently, doesn't test anything. Need to add optional
                    #       argument to wait when removing workers once distributed
                    #       PR github.com/dask/distributed/pull/6377 is merged.
                    await client.wait_for_workers(3)


@pytest.mark.anyio
async def test_scalesimplecluster_from_cluster_spec(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            scheduler_deployment_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"
            while scheduler_deployment_name not in k8s_cluster.kubectl(
                "get", "pods", "-n", ns
            ):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc", "-n", ns):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods", "-n", ns):
                await asyncio.sleep(0.1)

            with k8s_cluster.port_forward(
                f"service/{service_name}", 8786, "-n", ns
            ) as port:
                async with Client(
                    f"tcp://localhost:{port}", asynchronous=True
                ) as client:
                    k8s_cluster.kubectl(
                        "scale",
                        "-n",
                        ns,
                        "--replicas=5",
                        "daskcluster.kubernetes.dask.org",
                        cluster_name,
                    )
                    await client.wait_for_workers(5)
                    k8s_cluster.kubectl(
                        "scale",
                        "-n",
                        ns,
                        "--replicas=3",
                        "daskcluster.kubernetes.dask.org",
                        cluster_name,
                    )
                    # TODO: Currently, doesn't test anything. Need to add optional
                    #       argument to wait when removing workers once distributed
                    #       PR github.com/dask/distributed/pull/6377 is merged.
                    await client.wait_for_workers(3)


@pytest.mark.anyio
async def test_recreate_scheduler_pod(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            scheduler_deployment_name = "simple-scheduler"
            worker_pod_name = "simple-default-worker"
            service_name = "simple-scheduler"
            while scheduler_deployment_name not in k8s_cluster.kubectl(
                "get", "pods", "-n", ns
            ):
                await asyncio.sleep(0.1)
            while service_name not in k8s_cluster.kubectl("get", "svc", "-n", ns):
                await asyncio.sleep(0.1)
            while worker_pod_name not in k8s_cluster.kubectl("get", "pods", "-n", ns):
                await asyncio.sleep(0.1)
            k8s_cluster.kubectl(
                "delete",
                "pods",
                "-l",
                "dask.org/cluster-name=simple,dask.org/component=scheduler",
                "-n",
                ns,
            )
            k8s_cluster.kubectl(
                "wait",
                "--for=condition=Ready",
                "-l",
                "dask.org/cluster-name=simple,dask.org/component=scheduler",
                "pod",
                "-n",
                ns,
                "--timeout=60s",
            )
            assert scheduler_deployment_name in k8s_cluster.kubectl(
                "get", "pods", "-n", ns
            )


@pytest.mark.anyio
@pytest.mark.skip(reason="Flaky in CI")
async def test_recreate_worker_pods(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            cluster = await DaskCluster.get(cluster_name, namespace=ns)
            # Get the default worker group
            while not (wgs := await cluster.worker_groups()):
                await asyncio.sleep(0.1)
            [wg] = wgs
            # Wait for worker Pods to be created
            while not (pods := await wg.pods()):
                await asyncio.sleep(0.1)
            # Store number of workers
            n_pods = len(pods)
            # Wait for worker Pods to be ready
            await asyncio.gather(
                *[pod.wait(conditions="condition=Ready", timeout=60) for pod in pods]
            )
            # Delete a worker Pod
            await pods[0].delete()
            # Wait for Pods to be recreated
            while len((pods := await wg.pods())) < n_pods:
                await asyncio.sleep(0.1)
            # Wait for worker Pods to be ready
            await asyncio.gather(
                *[pod.wait(conditions="condition=Ready", timeout=60) for pod in pods]
            )


@pytest.mark.anyio
async def test_simplecluster_batched_worker_deployments(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        with dask.config.set(
            {
                "kubernetes.controller.worker-allocation.batch-size": 1,
                "kubernetes.controller.worker-allocation.delay": 5,
            }
        ):
            async with gen_cluster() as (cluster_name, ns):
                scheduler_deployment_name = "simple-scheduler"
                worker_pod_name = "simple-default-worker"
                service_name = "simple-scheduler"
                while scheduler_deployment_name not in k8s_cluster.kubectl(
                    "get", "deployments", "-n", ns
                ):
                    await asyncio.sleep(0.1)
                while service_name not in k8s_cluster.kubectl("get", "svc", "-n", ns):
                    await asyncio.sleep(0.1)
                while worker_pod_name not in k8s_cluster.kubectl(
                    "get", "pods", "-n", ns
                ):
                    await asyncio.sleep(0.1)

                with k8s_cluster.port_forward(
                    f"service/{service_name}", 8786, "-n", ns
                ) as port:
                    async with Client(
                        f"tcp://localhost:{port}", asynchronous=True
                    ) as client:
                        await client.wait_for_workers(2)
                        futures = client.map(lambda x: x + 1, range(10))
                        total = client.submit(sum, futures)
                        assert (await total) == sum(map(lambda x: x + 1, range(10)))


def _get_job_status(k8s_cluster: KindCluster, ns: str) -> dict[str, Any]:
    return json.loads(  # type: ignore[no-any-return]
        k8s_cluster.kubectl(
            "get",
            "-n",
            ns,
            "daskjobs.kubernetes.dask.org",
            "-o",
            "jsonpath='{.items[0].status}'",
        )[1:-1]
    )


def _assert_job_status_created(job_status: dict[str, Any]) -> None:
    assert "jobStatus" in job_status


def _assert_job_status_cluster_created(job: str, job_status: dict[str, Any]) -> None:
    assert "jobStatus" in job_status
    assert job_status["clusterName"] == job
    assert job_status["jobRunnerPodName"] == get_job_runner_pod_name(job)


def _assert_job_status_running(job: str, job_status: dict[str, Any]) -> None:
    assert "jobStatus" in job_status
    assert job_status["clusterName"] == job
    assert job_status["jobRunnerPodName"] == get_job_runner_pod_name(job)
    start_time = datetime.strptime(job_status["startTime"], KUBERNETES_DATETIME_FORMAT)
    assert datetime.utcnow() > start_time > (datetime.utcnow() - timedelta(seconds=10))


def _assert_final_job_status(
    job: str, job_status: dict[str, Any], expected_status: str
) -> None:
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


@pytest.mark.anyio
async def test_job(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_job: Callable[[str], AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner as runner:
        async with gen_job("simplejob.yaml") as (job, ns):
            assert job

            runner_name = f"{job}-runner"

            # Assert that job was created
            while job not in k8s_cluster.kubectl(
                "get", "daskjobs.kubernetes.dask.org", "-n", ns
            ):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_created(job_status)

            # Assert that cluster is created
            while job not in k8s_cluster.kubectl(
                "get", "daskclusters.kubernetes.dask.org", "-n", ns
            ):
                await asyncio.sleep(0.1)

            await asyncio.sleep(0.1)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_cluster_created(job, job_status)

            # Assert job pod is created
            while job not in k8s_cluster.kubectl("get", "po", "-n", ns):
                await asyncio.sleep(0.1)

            # Assert that pod started Running
            while "Running" not in k8s_cluster.kubectl("get", "po", "-n", ns):
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_running(job, job_status)

            job_annotations = json.loads(
                k8s_cluster.kubectl(
                    "get",
                    "pods",
                    "-n",
                    ns,
                    "--selector=dask.org/component=job-runner",
                    "-o",
                    "jsonpath='{.items[0].metadata.annotations}'",
                )[1:-1]
            )
            assert _EXPECTED_ANNOTATIONS.items() <= job_annotations.items()

            # Assert job pod runs to completion (will fail if doesn't connect to cluster)
            while "Completed" not in k8s_cluster.kubectl(
                "get", "-n", ns, "po", runner_name
            ):
                await asyncio.sleep(0.1)

            # Assert cluster is removed on completion
            while job in k8s_cluster.kubectl(
                "get", "-n", ns, "daskclusters.kubernetes.dask.org"
            ):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster, ns)
            _assert_final_job_status(job, job_status, "Successful")

    assert "A DaskJob has been created" in runner.stdout
    assert "Job succeeded, deleting Dask cluster." in runner.stdout


@pytest.mark.anyio
async def test_failed_job(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_job: Callable[[str], AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner as runner:
        async with gen_job("failedjob.yaml") as (job, ns):
            assert job

            runner_name = f"{job}-runner"

            # Assert that job was created
            while job not in k8s_cluster.kubectl(
                "get", "daskjobs.kubernetes.dask.org", "-n", ns
            ):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_created(job_status)

            # Assert that cluster is created
            while job not in k8s_cluster.kubectl(
                "get", "daskclusters.kubernetes.dask.org", "-n", ns
            ):
                await asyncio.sleep(0.1)

            await asyncio.sleep(0.1)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_cluster_created(job, job_status)

            # Assert job pod is created
            while job not in k8s_cluster.kubectl("get", "po", "-n", ns):
                await asyncio.sleep(0.1)

            # Assert that pod started Running
            while "Running" not in k8s_cluster.kubectl("get", "po", "-n", ns):
                await asyncio.sleep(0.1)

            await asyncio.sleep(5)  # Wait for a short time, to avoid race condition
            job_status = _get_job_status(k8s_cluster, ns)
            _assert_job_status_running(job, job_status)

            # Assert job pod runs to failure
            while "Error" not in k8s_cluster.kubectl(
                "get", "po", "-n", ns, runner_name
            ):
                await asyncio.sleep(0.1)

            # Assert cluster is removed on completion
            while job in k8s_cluster.kubectl(
                "get", "-n", ns, "daskclusters.kubernetes.dask.org"
            ):
                await asyncio.sleep(0.1)

            job_status = _get_job_status(k8s_cluster, ns)
            _assert_final_job_status(job, job_status, "Failed")

    assert "A DaskJob has been created" in runner.stdout
    assert "Job failed, deleting Dask cluster." in runner.stdout


@pytest.mark.anyio
async def test_object_dask_cluster(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster() as (cluster_name, ns):
            cluster = await DaskCluster.get(cluster_name, namespace=ns)

            worker_groups: list[DaskWorkerGroup] = []
            while not worker_groups:
                worker_groups = await cluster.worker_groups()
                await asyncio.sleep(0.1)
            assert len(worker_groups) == 1  # Just the default worker group
            wg = worker_groups[0]
            assert isinstance(wg, DaskWorkerGroup)

            # Test for non-replicated environment variables; Fix for https://github.com/dask/dask-kubernetes/issues/841
            for deployment in await wg.deployments():
                env_vars = deployment.spec["template"]["spec"]["containers"]["env"]
                env_var_names = [env_var["name"] for env_var in env_vars]
                assert len(env_var_names) == len(set(env_var_names))
                assert "DASK_WORKER_NAME" in env_var_names
                assert "DASK_SCHEDULER_ADDRESS" in env_var_names

            scheduler_pod = await cluster.scheduler_pod()
            assert isinstance(scheduler_pod, Pod)

            scheduler_deployment = await cluster.scheduler_deployment()
            assert isinstance(scheduler_deployment, Deployment)

            scheduler_service = await cluster.scheduler_service()
            assert isinstance(scheduler_service, Service)


@pytest.mark.anyio
async def test_object_dask_worker_group(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
    gen_worker_group: Callable[[str], AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with (
            gen_cluster() as (cluster_name, ns),
            gen_worker_group("simpleworkergroup.yaml") as (
                additional_workergroup_name,
                _,
            ),
        ):
            cluster = await DaskCluster.get(cluster_name, namespace=ns)
            additional_workergroup = await DaskWorkerGroup.get(
                additional_workergroup_name, namespace=ns
            )

            worker_groups: list[DaskWorkerGroup] = []
            while not worker_groups:
                worker_groups = await cluster.worker_groups()
                await asyncio.sleep(0.1)
            assert len(worker_groups) == 1  # Just the default worker group
            worker_groups = worker_groups + [additional_workergroup]

            for wg in worker_groups:
                assert isinstance(wg, DaskWorkerGroup)

                deployments: list[Deployment] = []
                while not deployments:
                    deployments = await wg.deployments()
                    await asyncio.sleep(0.1)
                assert all([isinstance(d, Deployment) for d in deployments])

                pods: list[Pod] = []
                while not pods:
                    pods = await wg.pods()
                    await asyncio.sleep(0.1)
                assert all([isinstance(p, Pod) for p in pods])

                assert (await wg.cluster()).name == cluster.name

                for deployment in deployments:
                    assert deployment.labels["dask.org/cluster-name"] == cluster.name
                    for env in deployment.spec["template"]["spec"]["containers"][0][
                        "env"
                    ]:
                        if env["name"] == "DASK_WORKER_NAME":
                            if wg.name == additional_workergroup_name:
                                assert env["value"] == "test-worker"
                            else:
                                assert env["value"] == deployment.name
                        if env["name"] == "DASK_SCHEDULER_ADDRESS":
                            scheduler_service = await cluster.scheduler_service()
                            assert f"{scheduler_service.name}.{ns}" in env["value"]


@pytest.mark.anyio
@pytest.mark.skip(reason="Flaky in CI")
async def test_object_dask_job(
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_job: Callable[[str], AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_job("simplejob.yaml") as (job_name, ns):
            job = await DaskJob.get(job_name, namespace=ns)

            job_pod = await job.pod()
            assert isinstance(job_pod, Pod)

            cluster = await job.cluster()
            assert isinstance(cluster, DaskCluster)


async def _get_cluster_status(
    k8s_cluster: KindCluster, ns: str, cluster_name: str
) -> str:
    """
    Will loop infinitely in search of non-falsey cluster status.
    Make sure there is a timeout on any test which calls this.
    """
    while True:
        cluster_status: str = k8s_cluster.kubectl(
            "get",
            "-n",
            ns,
            "daskcluster.kubernetes.dask.org",
            cluster_name,
            "-o",
            "jsonpath='{.status.phase}'",
        ).strip("'")
        if cluster_status:
            return cluster_status
        await asyncio.sleep(0.1)


@pytest.mark.timeout(180)
@pytest.mark.anyio
@pytest.mark.parametrize(
    "cluster_name,expected_status",
    [
        ("valid-name", "Created"),
        ((MAX_CLUSTER_NAME_LEN + 1) * "a", "Error"),
        ("invalid.chars.in.name", "Error"),
    ],
)
async def test_create_cluster_validates_name(
    cluster_name: str,
    expected_status: str,
    k8s_cluster: KindCluster,
    kopf_runner: KopfRunner,
    gen_cluster: Callable[..., AsyncContextManager[tuple[str, str]]],
) -> None:
    with kopf_runner:
        async with gen_cluster(cluster_name=cluster_name) as (_, ns):
            actual_status = await _get_cluster_status(k8s_cluster, ns, cluster_name)
            assert expected_status == actual_status
