import asyncio
from collections import defaultdict
import time
from contextlib import suppress
from datetime import datetime
from uuid import uuid4

import aiohttp
import kopf
import kubernetes_asyncio as kubernetes
from importlib_metadata import entry_points
from kubernetes_asyncio.client import ApiException

from dask_kubernetes.operator._objects import DaskCluster
from dask_kubernetes.common.auth import ClusterAuth
from dask_kubernetes.common.networking import get_scheduler_address
from distributed.core import rpc, clean_exception
from distributed.protocol.pickle import dumps

_ANNOTATION_NAMESPACES_TO_IGNORE = (
    "kopf.zalando.org",
    "kubectl.kubernetes.io",
)
_LABEL_NAMESPACES_TO_IGNORE = ()

KUBERNETES_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DASK_AUTOSCALER_COOLDOWN_UNTIL_ANNOTATION = "kubernetes.dask.org/cooldown-until"

# Load operator plugins from other packages
PLUGINS = []
for ep in entry_points(group="dask_operator_plugin"):
    with suppress(AttributeError, ImportError):
        PLUGINS.append(ep.load())


class SchedulerCommError(Exception):
    """Raised when unable to communicate with a scheduler."""


def _get_annotations(meta):
    return {
        annotation_key: annotation_value
        for annotation_key, annotation_value in meta.annotations.items()
        if not any(
            annotation_key.startswith(namespace)
            for namespace in _ANNOTATION_NAMESPACES_TO_IGNORE
        )
    }


def _get_labels(meta):
    return {
        label_key: label_value
        for label_key, label_value in meta.labels.items()
        if not any(
            label_key.startswith(namespace) for namespace in _LABEL_NAMESPACES_TO_IGNORE
        )
    }


def build_scheduler_deployment_spec(
    cluster_name, namespace, pod_spec, annotations, labels
):
    labels.update(
        **{
            "dask.org/cluster-name": cluster_name,
            "dask.org/component": "scheduler",
            "sidecar.istio.io/inject": "false",
        }
    )
    metadata = {
        "name": f"{cluster_name}-scheduler",
        "labels": labels,
        "annotations": annotations,
    }
    spec = {}
    spec["replicas"] = 1
    spec["selector"] = {
        "matchLabels": labels,
    }
    spec["template"] = {
        "metadata": metadata,
        "spec": pod_spec,
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": metadata,
        "spec": spec,
    }


def build_scheduler_service_spec(cluster_name, spec, annotations, labels):
    labels.update(
        **{
            "dask.org/cluster-name": cluster_name,
            "dask.org/component": "scheduler",
        }
    )
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": f"{cluster_name}-scheduler",
            "labels": labels,
            "annotations": annotations,
        },
        "spec": spec,
    }


def build_worker_deployment_spec(
    worker_group_name, namespace, cluster_name, uuid, pod_spec, annotations, labels
):
    labels.update(
        **{
            "dask.org/cluster-name": cluster_name,
            "dask.org/workergroup-name": worker_group_name,
            "dask.org/component": "worker",
            "sidecar.istio.io/inject": "false",
        }
    )
    worker_name = f"{worker_group_name}-worker-{uuid}"
    metadata = {
        "name": worker_name,
        "labels": labels,
        "annotations": annotations,
    }
    spec = {}
    spec["replicas"] = 1  # make_worker_spec returns dict with a replicas key?
    spec["selector"] = {
        "matchLabels": labels,
    }
    spec["template"] = {
        "metadata": metadata,
        "spec": pod_spec,
    }
    deployment_spec = {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": metadata,
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
    for i in range(len(deployment_spec["spec"]["template"]["spec"]["containers"])):
        if "env" in deployment_spec["spec"]["template"]["spec"]["containers"][i]:
            deployment_spec["spec"]["template"]["spec"]["containers"][i]["env"].extend(
                env
            )
        else:
            deployment_spec["spec"]["template"]["spec"]["containers"][i]["env"] = env
    return deployment_spec


def get_job_runner_pod_name(job_name):
    return f"{job_name}-runner"


def build_job_pod_spec(job_name, cluster_name, namespace, spec, annotations, labels):
    labels.update(
        **{
            "dask.org/cluster-name": cluster_name,
            "dask.org/component": "job-runner",
            "sidecar.istio.io/inject": "false",
        }
    )
    pod_spec = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": get_job_runner_pod_name(job_name),
            "labels": labels,
            "annotations": annotations,
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


def build_default_worker_group_spec(cluster_name, spec, annotations, labels):
    labels.update(
        **{
            "dask.org/cluster-name": cluster_name,
            "dask.org/component": "workergroup",
        }
    )
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {
            "name": f"{cluster_name}-default",
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "cluster": cluster_name,
            "worker": spec,
        },
    }


def build_cluster_spec(name, worker_spec, scheduler_spec, annotations, labels):
    labels.update(
        **{
            "dask.org/cluster-name": name,
        }
    )
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {
            "name": name,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {"worker": worker_spec, "scheduler": scheduler_spec},
    }


@kopf.on.startup()
async def startup(settings: kopf.OperatorSettings, **kwargs):
    # Authenticate with k8s
    await ClusterAuth.load_first()

    # Set server and client timeouts to reconnect from time to time.
    # In rare occasions the connection might go idle we will no longer receive any events.
    # These timeouts should help in those cases.
    # https://github.com/nolar/kopf/issues/698
    # https://github.com/nolar/kopf/issues/204
    settings.watching.server_timeout = 120
    settings.watching.client_timeout = 150
    settings.watching.connect_timeout = 5

    # The default timeout is 300s which is usually to long
    # https://kopf.readthedocs.io/en/latest/configuration/#networking-timeouts
    settings.networking.request_timeout = 10


# There may be useful things for us to expose via the liveness probe
# https://kopf.readthedocs.io/en/stable/probing/#probe-handlers
@kopf.on.probe(id="now")
def get_current_timestamp(**kwargs):
    return datetime.utcnow().isoformat()


@kopf.on.create("daskcluster.kubernetes.dask.org")
async def daskcluster_create(name, namespace, logger, patch, **kwargs):
    """When DaskCluster resource is created set the status.phase.

    This allows us to track that the operator is running.
    """
    logger.info(f"DaskCluster {name} created in {namespace}.")
    patch.status["phase"] = "Created"


@kopf.on.field("daskcluster.kubernetes.dask.org", field="status.phase", new="Created")
async def daskcluster_create_components(
    spec, name, namespace, logger, patch, meta, **kwargs
):
    """When the DaskCluster status.phase goes into Created create the cluster components."""
    logger.info("Creating Dask cluster components.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        custom_api = kubernetes.client.CustomObjectsApi(api_client)

        annotations = _get_annotations(meta)
        labels = _get_labels(meta)
        scheduler_spec = spec.get("scheduler", {})
        if "metadata" in scheduler_spec:
            if "annotations" in scheduler_spec["metadata"]:
                annotations.update(**scheduler_spec["metadata"]["annotations"])
            if "labels" in scheduler_spec["metadata"]:
                labels.update(**scheduler_spec["metadata"]["labels"])
        data = build_scheduler_deployment_spec(
            name, namespace, scheduler_spec.get("spec"), annotations, labels
        )
        kopf.adopt(data)
        pod = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={name}",
        )
        if not pod.items:
            await kubernetes.client.AppsV1Api(api_client).create_namespaced_deployment(
                namespace=namespace,
                body=data,
            )
            logger.info(
                f"Scheduler deployment {data['metadata']['name']} created in {namespace}."
            )

        data = build_scheduler_service_spec(
            name, scheduler_spec.get("service"), annotations, labels
        )
        kopf.adopt(data)
        service = await api.list_namespaced_service(
            namespace=namespace,
            label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={name}",
        )
        if not service.items:
            await api.create_namespaced_service(
                namespace=namespace,
                body=data,
            )
        logger.info(
            f"Scheduler service {data['metadata']['name']} created in {namespace}."
        )

        worker_spec = spec.get("worker", {})
        annotations = _get_annotations(meta)
        labels = _get_labels(meta)
        if "metadata" in worker_spec:
            if "annotations" in worker_spec["metadata"]:
                annotations.update(**worker_spec["metadata"]["annotations"])
            if "labels" in worker_spec["metadata"]:
                labels.update(**worker_spec["metadata"]["labels"])
        data = build_default_worker_group_spec(name, worker_spec, annotations, labels)
        worker_group = await custom_api.list_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            label_selector=f"dask.org/component=workergroup,dask.org/cluster-name={name}",
        )
        if not worker_group["items"]:
            await custom_api.create_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=namespace,
                body=data,
            )
            logger.info(
                f"Worker group {data['metadata']['name']} created in {namespace}."
            )
    patch.status["phase"] = "Pending"


@kopf.on.field("service", field="status", labels={"dask.org/component": "scheduler"})
async def handle_scheduler_service_status(
    spec, labels, status, namespace, logger, **kwargs
):
    # If the Service is a LoadBalancer with no ingress endpoints mark the cluster as Pending
    if spec["type"] == "LoadBalancer" and not len(
        status.get("load_balancer", {}).get("ingress", [])
    ):
        phase = "Pending"
    # Otherwise mark it as Running
    else:
        phase = "Running"
    cluster = await DaskCluster.get(
        labels["dask.org/cluster-name"], namespace=namespace
    )
    await cluster.patch({"status": {"phase": phase}})


@kopf.on.create("daskworkergroup.kubernetes.dask.org")
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

    del kwargs["new"]
    await daskworkergroup_replica_update(
        spec=spec,
        name=name,
        namespace=namespace,
        logger=logger,
        new=spec["worker"]["replicas"],
        **kwargs,
    )


async def retire_workers(
    n_workers, scheduler_service_name, worker_group_name, namespace, logger
):
    # Try gracefully retiring via the HTTP API
    dashboard_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        port_name="http-dashboard",
        allow_external=False,
    )
    async with aiohttp.ClientSession() as session:
        url = f"{dashboard_address}/api/v1/retire_workers"
        params = {"n": n_workers}
        async with session.post(url, json=params) as resp:
            if resp.status <= 300:
                retired_workers = await resp.json()
                logger.info("Retired workers %s", retired_workers)
                return [retired_workers[w]["name"] for w in retired_workers.keys()]
            logger.debug(
                "Received %d response from scheduler API with body %s",
                resp.status,
                await resp.text(),
            )

    # Otherwise try gracefully retiring via the RPC
    logger.debug(
        f"Scaling {worker_group_name} failed via the HTTP API, falling back to the Dask RPC"
    )
    # Dask version mismatches between the operator and scheduler may cause this to fail in any number of unexpected ways
    with suppress(Exception):
        comm_address = await get_scheduler_address(
            scheduler_service_name,
            namespace,
            allow_external=False,
        )
        async with rpc(comm_address) as scheduler_comm:
            workers_to_close = await scheduler_comm.workers_to_close(
                n=n_workers,
                attribute="name",
            )
            await scheduler_comm.retire_workers(names=workers_to_close)
            return workers_to_close

    # Finally fall back to last-in-first-out scaling
    logger.debug(
        f"Scaling {worker_group_name} failed via the Dask RPC, falling back to LIFO scaling"
    )
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={worker_group_name}",
        )
        return [w["metadata"]["name"] for w in workers.items[:-n_workers]]


async def check_scheduler_idle(scheduler_service_name, namespace, logger):
    # Try getting idle time via HTTP API
    dashboard_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        port_name="http-dashboard",
        allow_external=False,
    )
    async with aiohttp.ClientSession() as session:
        url = f"{dashboard_address}/api/v1/check_idle"
        async with session.get(url) as resp:
            if resp.status <= 300:
                idle_since = (await resp.json())["idle_since"]
                if idle_since:
                    logger.debug("Scheduler idle since: %s", idle_since)
                return idle_since
            logger.debug(
                "Received %d response from scheduler API with body %s",
                resp.status,
                await resp.text(),
            )

    # Otherwise try gracefully checking via the RPC
    logger.debug(
        f"Checking {scheduler_service_name} idleness failed via the HTTP API, falling back to the Dask RPC"
    )
    # Dask version mismatches between the operator and scheduler may cause this to fail in any number of unexpected ways
    with suppress(Exception):
        comm_address = await get_scheduler_address(
            scheduler_service_name,
            namespace,
            allow_external=False,
        )
        async with rpc(comm_address) as scheduler_comm:
            idle_since = await scheduler_comm.check_idle()
            if idle_since:
                logger.debug("Scheduler idle since: %s", idle_since)
            return idle_since

    # Finally fall back to code injection via the Dask RPC for distributed<=2023.3.1
    logger.debug(
        f"Checking {scheduler_service_name} idleness failed via the Dask RPC, falling back to run_on_scheduler"
    )

    def idle_since(dask_scheduler=None):
        if not dask_scheduler.idle_timeout:
            dask_scheduler.idle_timeout = 300
        dask_scheduler.check_idle()
        return dask_scheduler.idle_since

    comm_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        allow_external=False,
    )
    async with rpc(comm_address) as scheduler_comm:
        response = await scheduler_comm.run_function(
            function=dumps(idle_since),
        )
        if response["status"] == "error":
            typ, exc, tb = clean_exception(**response)
            raise exc.with_traceback(tb)
        else:
            idle_since = response["result"]
            if idle_since:
                logger.debug("Scheduler idle since: %s", idle_since)
            return idle_since


async def get_desired_workers(scheduler_service_name, namespace, logger):
    # Try gracefully retiring via the HTTP API
    dashboard_address = await get_scheduler_address(
        scheduler_service_name,
        namespace,
        port_name="http-dashboard",
        allow_external=False,
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
            allow_external=False,
        )
        async with rpc(comm_address) as scheduler_comm:
            return await scheduler_comm.adaptive_target()
    except Exception as e:
        raise SchedulerCommError(
            "Unable to get number of desired workers from scheduler"
        ) from e


worker_group_scale_locks = defaultdict(lambda: asyncio.Lock())


@kopf.on.field("daskcluster.kubernetes.dask.org", field="spec.worker.replicas")
async def daskcluster_default_worker_group_replica_update(
    name, namespace, meta, spec, old, new, body, logger, **kwargs
):
    if old is None:
        return
    worker_group_name = f"{name}-default"

    async with kubernetes.client.api_client.ApiClient() as api_client:
        custom_objects_api = kubernetes.client.CustomObjectsApi(api_client)
        custom_objects_api.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )
        await custom_objects_api.patch_namespaced_custom_object_scale(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=worker_group_name,
            body={"spec": {"replicas": new}},
        )


@kopf.on.field("daskworkergroup.kubernetes.dask.org", field="spec.worker.replicas")
async def daskworkergroup_replica_update(
    name, namespace, meta, spec, new, body, logger, **kwargs
):
    cluster_name = spec["cluster"]

    # Replica updates can come in quick succession and the changes must be applied atomically to ensure
    # the number of workers ends in the correct state
    async with worker_group_scale_locks[f"{namespace}/{name}"]:
        async with kubernetes.client.api_client.ApiClient() as api_client:
            customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
            corev1api = kubernetes.client.CoreV1Api(api_client)

            try:
                cluster = await customobjectsapi.get_namespaced_custom_object(
                    group="kubernetes.dask.org",
                    version="v1",
                    plural="daskclusters",
                    namespace=namespace,
                    name=cluster_name,
                )
            except ApiException as e:
                if e.status == 404:
                    # No need to scale if worker group is deleted, pods will be cleaned up
                    return
                else:
                    raise e

            cluster_labels = cluster.get("metadata", {}).get("labels", {})

            workers = await corev1api.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"dask.org/workergroup-name={name}",
            )
            current_workers = len(
                [w for w in workers.items if w.status.phase != "Terminating"]
            )
            desired_workers = new
            workers_needed = desired_workers - current_workers
            labels = _get_labels(meta)
            annotations = _get_annotations(meta)
            worker_spec = spec["worker"]
            if "metadata" in worker_spec:
                if "annotations" in worker_spec["metadata"]:
                    annotations.update(**worker_spec["metadata"]["annotations"])
                if "labels" in worker_spec["metadata"]:
                    labels.update(**worker_spec["metadata"]["labels"])
            if workers_needed > 0:
                for _ in range(workers_needed):
                    data = build_worker_deployment_spec(
                        worker_group_name=name,
                        namespace=namespace,
                        cluster_name=cluster_name,
                        uuid=uuid4().hex[:10],
                        pod_spec=worker_spec["spec"],
                        annotations=annotations,
                        labels=labels,
                    )
                    kopf.adopt(data, owner=body)
                    kopf.label(data, labels=cluster_labels)
                    await kubernetes.client.AppsV1Api(
                        api_client
                    ).create_namespaced_deployment(
                        namespace=namespace,
                        body=data,
                    )
                logger.info(
                    f"Scaled worker group {name} up to {desired_workers} workers."
                )
            if workers_needed < 0:
                worker_ids = await retire_workers(
                    n_workers=-workers_needed,
                    scheduler_service_name=f"{cluster_name}-scheduler",
                    worker_group_name=name,
                    namespace=namespace,
                    logger=logger,
                )
                logger.info(f"Workers to close: {worker_ids}")
                for wid in worker_ids:
                    await kubernetes.client.AppsV1Api(
                        api_client
                    ).delete_namespaced_deployment(
                        name=wid,
                        namespace=namespace,
                    )
                logger.info(
                    f"Scaled worker group {name} down to {desired_workers} workers."
                )


@kopf.on.delete("daskworkergroup.kubernetes.dask.org", optional=True)
async def daskworkergroup_remove(name, namespace, **kwargs):
    lock_key = f"{name}/{namespace}"
    if lock_key in worker_group_scale_locks:
        del worker_group_scale_locks[lock_key]


@kopf.on.create("daskjob.kubernetes.dask.org")
async def daskjob_create(name, namespace, logger, patch, **kwargs):
    logger.info(f"A DaskJob has been created called {name} in {namespace}.")
    patch.status["jobStatus"] = "JobCreated"


@kopf.on.field(
    "daskjob.kubernetes.dask.org", field="status.jobStatus", new="JobCreated"
)
async def daskjob_create_components(
    spec, name, namespace, logger, patch, meta, **kwargs
):
    logger.info("Creating Dask job components.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        corev1api = kubernetes.client.CoreV1Api(api_client)

        cluster_name = f"{name}"
        labels = _get_labels(meta)
        annotations = _get_annotations(meta)
        cluster_spec = spec["cluster"]
        if "metadata" in cluster_spec:
            if "annotations" in cluster_spec["metadata"]:
                annotations.update(**cluster_spec["metadata"]["annotations"])
            if "labels" in cluster_spec["metadata"]:
                labels.update(**cluster_spec["metadata"]["labels"])
        cluster_spec = build_cluster_spec(
            cluster_name,
            cluster_spec["spec"]["worker"],
            cluster_spec["spec"]["scheduler"],
            annotations,
            labels,
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

        labels = _get_labels(meta)
        annotations = _get_annotations(meta)
        job_spec = spec["job"]
        if "metadata" in job_spec:
            if "annotations" in job_spec["metadata"]:
                annotations.update(**job_spec["metadata"]["annotations"])
            if "labels" in job_spec["metadata"]:
                labels.update(**job_spec["metadata"]["labels"])
        job_pod_spec = build_job_pod_spec(
            job_name=name,
            cluster_name=cluster_name,
            namespace=namespace,
            spec=job_spec["spec"],
            annotations=annotations,
            labels=labels,
        )
        kopf.adopt(job_pod_spec)
        await corev1api.create_namespaced_pod(
            namespace=namespace,
            body=job_pod_spec,
        )
        patch.status["clusterName"] = cluster_name
        patch.status["jobStatus"] = "ClusterCreated"
        patch.status["jobRunnerPodName"] = get_job_runner_pod_name(name)


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Running",
)
async def handle_runner_status_change_running(meta, namespace, logger, **kwargs):
    logger.info("Job now in running")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        api_client.set_default_header("content-type", "application/merge-patch+json")
        await customobjectsapi.patch_namespaced_custom_object_status(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskjobs",
            namespace=namespace,
            name=meta["labels"]["dask.org/cluster-name"],
            body={
                "status": {
                    "jobStatus": "Running",
                    "startTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
                }
            },
        )


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Succeeded",
)
async def handle_runner_status_change_succeeded(meta, namespace, logger, **kwargs):
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
        api_client.set_default_header("content-type", "application/merge-patch+json")
        await customobjectsapi.patch_namespaced_custom_object_status(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskjobs",
            namespace=namespace,
            name=meta["labels"]["dask.org/cluster-name"],
            body={
                "status": {
                    "jobStatus": "Successful",
                    "endTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
                }
            },
        )


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Failed",
)
async def handle_runner_status_change_succeeded(meta, namespace, logger, **kwargs):
    logger.info("Job failed, deleting Dask cluster.")
    async with kubernetes.client.api_client.ApiClient() as api_client:
        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        await customobjectsapi.delete_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskclusters",
            namespace=namespace,
            name=meta["labels"]["dask.org/cluster-name"],
        )
        api_client.set_default_header("content-type", "application/merge-patch+json")
        await customobjectsapi.patch_namespaced_custom_object_status(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskjobs",
            namespace=namespace,
            name=meta["labels"]["dask.org/cluster-name"],
            body={
                "status": {
                    "jobStatus": "Failed",
                    "endTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
                }
            },
        )


@kopf.on.create("daskautoscaler.kubernetes.dask.org")
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


@kopf.timer("daskautoscaler.kubernetes.dask.org", interval=5.0)
async def daskautoscaler_adapt(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        coreapi = kubernetes.client.CoreV1Api(api_client)

        pod_ready = False
        try:
            pods = await coreapi.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={spec['cluster']}",
            )
            scheduler_pod = await coreapi.read_namespaced_pod(
                pods.items[0].metadata.name, namespace
            )
            if scheduler_pod.status.phase == "Running":
                pod_ready = True
        except ApiException as e:
            if e.status != 404:
                raise e

        if not pod_ready:
            logger.info("Scheduler not ready, skipping autoscaling")
            return

        customobjectsapi = kubernetes.client.CustomObjectsApi(api_client)
        customobjectsapi.api_client.set_default_header(
            "content-type", "application/merge-patch+json"
        )

        autoscaler_resource = await customobjectsapi.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskautoscalers",
            namespace=namespace,
            name=name,
        )

        worker_group_resource = await customobjectsapi.get_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
            name=f"{spec['cluster']}-default",
        )

        current_replicas = int(worker_group_resource["spec"]["worker"]["replicas"])
        cooldown_until = float(
            autoscaler_resource.get("metadata", {})
            .get("annotations", {})
            .get(DASK_AUTOSCALER_COOLDOWN_UNTIL_ANNOTATION, time.time())
        )

        # Cooldown autoscaling to prevent thrashing
        if time.time() < cooldown_until:
            logger.debug("Autoscaler for %s is in cooldown", spec["cluster"])
            return

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

        if current_replicas > 0:
            max_scale_down = int(current_replicas * 0.25)
            max_scale_down = 1 if max_scale_down == 0 else max_scale_down
            desired_workers = max(current_replicas - max_scale_down, desired_workers)

        # Update the default DaskWorkerGroup
        if desired_workers != current_replicas:
            await customobjectsapi.patch_namespaced_custom_object_scale(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskworkergroups",
                namespace=namespace,
                name=f"{spec['cluster']}-default",
                body={"spec": {"replicas": desired_workers}},
            )

            cooldown_until = time.time() + 15

            await customobjectsapi.patch_namespaced_custom_object(
                group="kubernetes.dask.org",
                version="v1",
                plural="daskautoscalers",
                namespace=namespace,
                name=name,
                body={
                    "metadata": {
                        "annotations": {
                            DASK_AUTOSCALER_COOLDOWN_UNTIL_ANNOTATION: str(
                                cooldown_until
                            )
                        }
                    }
                },
            )

            logger.info(
                "Autoscaler updated %s worker count from %d to %d",
                spec["cluster"],
                current_replicas,
                desired_workers,
            )
        else:
            logger.debug(
                "Not autoscaling %s with %d workers", spec["cluster"], current_replicas
            )


@kopf.timer("daskcluster.kubernetes.dask.org", interval=5.0)
async def daskcluster_autoshutdown(spec, name, namespace, logger, **kwargs):
    if spec["idleTimeout"]:
        try:
            idle_since = await check_scheduler_idle(
                scheduler_service_name=f"{name}-scheduler",
                namespace=namespace,
                logger=logger,
            )
        except Exception as e:
            logger.warn("Unable to connect to scheduler, skipping autoshutdown check.")
            return
        if idle_since and time.time() > idle_since + spec["idleTimeout"]:
            cluster = await DaskCluster.get(name, namespace=namespace)
            await cluster.delete()
