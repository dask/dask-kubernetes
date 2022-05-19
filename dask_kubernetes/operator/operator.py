import asyncio
import aiohttp
import json

from distributed.core import rpc

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4

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


def build_worker_pod_spec(name, cluster_name, n, spec):
    worker_name = f"{name}-worker-{n}"
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": worker_name,
            "labels": {
                "dask.org/cluster-name": cluster_name,
                "dask.org/workergroup-name": name,
                "dask.org/component": "worker",
                "sidecar.istio.io/inject": "false",
            },
        },
        "spec": spec,
    }


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
                    name, spec["cluster"], uuid4().hex, spec["worker"]["spec"]
                )
                data["spec"]["containers"][0]["args"].append(
                    f"--name={data['metadata']['name']}"
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
            service_name = f"{name.split('-')[0]}-cluster-service"
            address = await get_scheduler_address(service_name, namespace)
            async with rpc(address) as scheduler:
                worker_ids = await scheduler.workers_to_close(
                    n=-workers_needed, attribute="name"
                )
            async with aiohttp.ClientSession() as session:
                params = {"n": -workers_needed}
                async with session.post(
                    "http://localhost:8787/api/v1/retire_workers", json=params
                ) as resp:
                    retired_workers = json.loads(await resp.text())["workers"]
                    logger.info(f"Retired workers API: {retired_workers}")
            # TODO: Check that were deting workers in the right worker group
            logger.info(f"Workers to close: {worker_ids}")
            for wid in worker_ids:
                await api.delete_namespaced_pod(
                    name=wid,
                    namespace=namespace,
                )
            logger.info(
                f"Scaled worker group {name} down to {spec['replicas']} workers."
            )
