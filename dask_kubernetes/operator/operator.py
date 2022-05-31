import asyncio
import aiohttp
from contextlib import suppress

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4

from distributed.core import rpc

from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.common.networking import (
    get_scheduler_address,
)


def build_scheduler_pod_spec(name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-scheduler",
            "labels": {
                "dask.org/cluster-name": name,
                "dask.org/component": "scheduler",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": spec,
    }


def build_scheduler_service_spec(name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": f"{name}-service",
            "labels": {
                "dask.org/cluster-name": name,
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
            "value": f"tcp://{cluster_name}-service.{namespace}.svc.cluster.local:8786",
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
            "value": f"tcp://{cluster_name}-service.{namespace}.svc.cluster.local:8786",
        },
    ]
    for i in range(len(pod_spec["spec"]["containers"])):
        if "env" in pod_spec["spec"]["containers"][i]:
            pod_spec["spec"]["containers"][i]["env"].extend(env)
        else:
            pod_spec["spec"]["containers"][i]["env"] = env
    return pod_spec


def build_worker_group_spec(name, spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {"name": f"{name}-default-worker-group"},
        "spec": {
            "cluster": name,
            "worker": spec,
        },
    }


def build_cluster_spec(name, worker_spec, scheduler_spec):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": name},
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
async def daskcluster_create(spec, name, namespace, logger, **kwargs):
    logger.info(
        f"A DaskCluster has been created called {name} in {namespace} with the following config: {spec}"
    )
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        # TODO Check for existing scheduler pod
        scheduler_spec = spec.get("scheduler", {})
        data = build_scheduler_pod_spec(name, scheduler_spec.get("spec"))
        kopf.adopt(data)
        await api.create_namespaced_pod(
            namespace=namespace,
            body=data,
        )
        # await wait_for_scheduler(name, namespace)
        logger.info(
            f"A scheduler pod has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )

        # TODO Check for existing scheduler service
        data = build_scheduler_service_spec(name, scheduler_spec.get("service"))
        kopf.adopt(data)
        await api.create_namespaced_service(
            namespace=namespace,
            body=data,
        )
        await wait_for_service(api, data["metadata"]["name"], namespace)
        logger.info(
            f"A scheduler service has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )

        worker_spec = spec.get("worker", {})
        data = build_worker_group_spec(name, worker_spec)
        # TODO: Next line is not needed if we can get worker groups adopted by the cluster
        kopf.adopt(data)
        api = kubernetes.client.CustomObjectsApi(api_client)
        await api.create_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            body=data,
        )
        logger.info(
            f"A worker group has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )


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
                scheduler_service_name=f"{spec['cluster']}-service",
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

        cluster_name = f"{name}-cluster"
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
            f"A cluster for the job {name} has been created called {cluster_spec['metadata']['name']} in {namespace}"
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


@kopf.on.field("pod", field="status.phase", labels={"dask.org/component": "job-runner"})
async def handle_runner_status_change(meta, new, namespace, logger, **kwargs):
    logger.info(f"Job now in phase {new}.")
    if new == "Succeeded":
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
