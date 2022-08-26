import asyncio
import aiohttp
from contextlib import suppress

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4

from dask.compatibility import entry_points
from distributed.core import rpc

from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.common.networking import (
    get_scheduler_address,
)

# Load operator plugins from other packages
PLUGINS = []
for ep in entry_points(group="dask_operator_plugin"):
    with suppress(AttributeError, ImportError):
        PLUGINS.append(ep.load())


class SchedulerCommError(Exception):
    """Raised when unable to communicate with a scheduler."""

    pass


def build_scheduler_pod_spec(cluster_name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{cluster_name}-scheduler",
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/component": "scheduler",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": spec,
    }


def build_scheduler_service_spec(cluster_name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": f"{cluster_name}-scheduler",
            "labels": {
                "dask.org/cluster-name": cluster_name,
            },
        },
        "spec": spec,
    }


def build_worker_pod_spec(worker_group_name, namespace, cluster_name, uuid, spec):
    worker_name = f"{worker_group_name}-worker-{uuid}"
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": worker_name,
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/workergroup-name": worker_group_name,
                "dask.org/component": "worker",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": spec,
    }
    env = [
        {
            "name": "DASK_WORKER_NAME",
            "value": worker_name,
        },
        {
            "name": "DASK_SCHEDULER_ADDRESS",
            "value": f"tcp://{cluster_name}-scheduler.{namespace}.svc.cluster.local:8786",
        },
    ]
    for i in range(len(pod_spec["spec"]["containers"])):
        if "env" in pod_spec["spec"]["containers"][i]:
            pod_spec["spec"]["containers"][i]["env"].extend(env)
        else:
            pod_spec["spec"]["containers"][i]["env"] = env
    return pod_spec


def build_job_pod_spec(job_name, cluster_name, namespace, spec):
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{job_name}-runner",
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/component": "job-runner",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": spec,
    }
    env = [
        {
            "name": "DASK_SCHEDULER_ADDRESS",
            "value": f"tcp://{cluster_name}-scheduler.{namespace}.svc.cluster.local:8786",
        },
    ]
    for i in range(len(pod_spec["spec"]["containers"])):
        if "env" in pod_spec["spec"]["containers"][i]:
            pod_spec["spec"]["containers"][i]["env"].extend(env)
        else:
            pod_spec["spec"]["containers"][i]["env"] = env
    return pod_spec


def build_default_worker_group_spec(cluster_name, spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {
            "name": f"{cluster_name}-default",
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/component": "workergroup",
            },
        },
        "spec": {
            "cluster": cluster_name,
            "worker": spec,
        },
    }


def build_cluster_spec(name, worker_spec, scheduler_spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {
            "name": name,
            "labels": {
                "dask.org/cluster-name": name,
            },
        },
        "spec": {"worker": worker_spec, "scheduler": scheduler_spec},
    }


async def wait_for_service(api, service_name, namespace):
    """Block until service is available."""
    while True:
        try:
            await api.read_namespaced_service(service_name, namespace)
            break
        except Exception:
            await asyncio.sleep(0.1)


@kopf.on.startup()
async def startup(**kwargs):
    await ClusterAuth.load_first()


@kopf.on.create("daskcluster")
async def daskcluster_create(name, namespace, logger, patch, **kwargs):
    """When DaskCluster resource is created set the status.phase.

    This allows us to track that the operator is running.
    """
    logger.info(f"DaskCluster {name} created in {namespace}.")
    patch.status["phase"] = "Created"


@kopf.on.field("daskcluster", field="status.phase", new="Created")
async def daskcluster_create_components(spec, name, namespace, logger, patch, **kwargs):
    """When the DaskCluster status.phase goes into Pending create the cluster components."""
    logger.info("Creating Dask cluster components.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        custom_api = kubernetes.client.CustomObjectsApi(api_client)

        # TODO Check for existing scheduler pod
        scheduler_spec = spec.get("scheduler", {})
        data = build_scheduler_pod_spec(name, scheduler_spec.get("spec"))
        kopf.adopt(data)
        await api.create_namespaced_pod(
            namespace=namespace,
            body=data,
        )
        # await wait_for_scheduler(name, namespace)
        logger.info(f"Scheduler pod {data['metadata']['name']} created in {namespace}.")

        # TODO Check for existing scheduler service
        data = build_scheduler_service_spec(name, scheduler_spec.get("service"))
        kopf.adopt(data)
        await api.create_namespaced_service(
            namespace=namespace,
            body=data,
        )
        await wait_for_service(api, data["metadata"]["name"], namespace)
        logger.info(
            f"Scheduler service {data['metadata']['name']} created in {namespace}."
        )

        worker_spec = spec.get("worker", {})
        data = build_default_worker_group_spec(name, worker_spec)
        await custom_api.create_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            body=data,
        )
        logger.info(f"Worker group {data['metadata']['name']} created in {namespace}.")
    # TODO Set to "Pending" here and track scheduler readiness before finally setting to "Running"
    patch.status["phase"] = "Running"


@kopf.on.create("daskworkergroup")
async def daskworkergroup_create(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CustomObjectsApi(api_client)
        cluster = await api.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            name=spec["cluster"],
        )
        new_spec = dict(spec)
        kopf.adopt(new_spec, owner=cluster)
        api.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )
        await api.patch_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=name,
            body=new_spec,
        )
        logger.info(f"Successfully adopted by {spec['cluster']}")

    await daskworkergroup_update(
        spec=spec, name=name, namespace=namespace, logger=logger, **kwargs
    )


async def retire_workers(
    n_workers, scheduler_service_name, worker_group_name, namespace, logger
):
    # Try gracefully retiring via the HTTP API
    dashboard_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        port_name="http-dashboard",
    )
    async with aiohttp.ClientSession() as session:
        url = f"{dashboard_address}/api/v1/retire_workers"
        params = {"n": n_workers}
        async with session.post(url, json=params) as resp:
            if resp.status <= 300:
                retired_workers = await resp.json()
                return [retired_workers[w]["name"] for w in retired_workers.keys()]

    # Otherwise try gracefully retiring via the RPC
    logger.info(
        f"Scaling {worker_group_name} failed via the HTTP API, falling back to the Dask RPC"
    )
    # Dask version mismatches between the operator and scheduler may cause this to fail in any number of unexpected ways
    with suppress(Exception):
        comm_address = await get_scheduler_address(
            scheduler_service_name,
            namespace,
        )
        async with rpc(comm_address) as scheduler_comm:
            return await scheduler_comm.workers_to_close(
                n=n_workers,
                attribute="name",
            )

    # Finally fall back to last-in-first-out scaling
    logger.info(
        f"Scaling {worker_group_name} failed via the Dask RPC, falling back to LIFO scaling"
    )
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={worker_group_name}",
        )
        return [w["metadata"]["name"] for w in workers.items[:-n_workers]]


async def get_desired_workers(scheduler_service_name, namespace, logger):
    # Try gracefully retiring via the HTTP API
    dashboard_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        port_name="http-dashboard",
    )
    async with aiohttp.ClientSession() as session:
        url = f"{dashboard_address}/api/v1/adaptive_target"
        async with session.get(url) as resp:
            if resp.status <= 300:
                desired_workers = await resp.json()
                return desired_workers["workers"]

    # Otherwise try gracefully retiring via the RPC
    # Dask version mismatches between the operator and scheduler may cause this to fail in any number of unexpected ways
    try:
        comm_address = await get_scheduler_address(
            scheduler_service_name,
            namespace,
        )
        async with rpc(comm_address) as scheduler_comm:
            return await scheduler_comm.adaptive_target()
    except Exception as e:
        raise SchedulerCommError(
            "Unable to get number of desired workers from scheduler"
        ) from e


@kopf.on.update("daskworkergroup")
async def daskworkergroup_update(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={name}",
        )
        current_workers = len(workers.items)
        desired_workers = spec["worker"]["replicas"]
        workers_needed = desired_workers - current_workers

        if workers_needed > 0:
            for _ in range(workers_needed):
                data = build_worker_pod_spec(
                    worker_group_name=name,
                    namespace=namespace,
                    cluster_name=spec["cluster"],
                    uuid=uuid4().hex[:10],
                    spec=spec["worker"]["spec"],
                )
                kopf.adopt(data)
                await api.create_namespaced_pod(
                    namespace=namespace,
                    body=data,
                )
            logger.info(
                f"Scaled worker group {name} up to {spec['worker']['replicas']} workers."
            )
        if workers_needed < 0:
            worker_ids = await retire_workers(
                n_workers=-workers_needed,
                scheduler_service_name=f"{spec['cluster']}-scheduler",
                worker_group_name=name,
                namespace=namespace,
                logger=logger,
            )
            logger.info(f"Workers to close: {worker_ids}")
            for wid in worker_ids:
                await api.delete_namespaced_pod(
                    name=wid,
                    namespace=namespace,
                )
            logger.info(
                f"Scaled worker group {name} down to {spec['worker']['replicas']} workers."
            )


@kopf.on.create("daskjob")
async def daskjob_create(spec, name, namespace, logger, **kwargs):
    logger.info(f"A DaskJob has been created called {name} in {namespace}.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        corev1api = kubernetes.client.CoreV1Api(api_client)

        cluster_name = f"{name}"
        cluster_spec = build_cluster_spec(
            cluster_name,
            spec["cluster"]["spec"]["worker"],
            spec["cluster"]["spec"]["scheduler"],
        )
        kopf.adopt(cluster_spec)
        await customobjectsapi.create_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            body=cluster_spec,
        )
        logger.info(
            f"Cluster {cluster_spec['metadata']['name']} for job {name} created in {namespace}."
        )

        job_pod_spec = build_job_pod_spec(
            job_name=name,
            cluster_name=cluster_name,
            namespace=namespace,
            spec=spec["job"]["spec"],
        )
        kopf.adopt(job_pod_spec)
        await corev1api.create_namespaced_pod(
            namespace=namespace,
            body=job_pod_spec,
        )


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Succeeded",
)
async def handle_runner_status_change(meta, new, namespace, logger, **kwargs):
    logger.info("Job succeeded, deleting Dask cluster.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        await customobjectsapi.delete_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            name=meta["labels"]["dask.org/cluster-name"],
        )


@kopf.on.create("daskautoscaler")
async def daskautoscaler_create(spec, name, namespace, logger, **kwargs):
    """When an autoscaler is created make it a child of the associated cluster for cascade deletion."""
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CustomObjectsApi(api_client)
        cluster = await api.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            name=spec["cluster"],
        )
        new_spec = dict(spec)
        kopf.adopt(new_spec, owner=cluster)
        api.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )
        await api.patch_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskautoscalers",
            namespace=namespace,
            name=name,
            body=new_spec,
        )
        logger.info(f"Successfully adopted by {spec['cluster']}")


@kopf.timer("daskautoscaler", interval=5.0)
async def daskautoscaler_adapt(spec, name, namespace, logger, **kwargs):
    # Ask the scheduler for the desired number of worker
    try:
        desired_workers = await get_desired_workers(
            scheduler_service_name=f"{spec['cluster']}-scheduler",
            namespace=namespace,
            logger=logger,
        )
    except SchedulerCommError:
        logger.error("Unable to get desired number of workers from scheduler.")
        return

    # Ensure the desired number is within the min and max
    desired_workers = max(spec["minimum"], desired_workers)
    desired_workers = min(spec["maximum"], desired_workers)

    # Update the default DaskWorkerGroup
    # TODO Only update if the value has changed
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        customobjectsapi.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )
        await customobjectsapi.patch_namespaced_custom_object_scale(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=f"{spec['cluster']}-default",
            body={"spec": {"replicas": desired_workers}},
        )
