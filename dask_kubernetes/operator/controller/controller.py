import asyncio
import time
from collections import defaultdict
from contextlib import suppress
from datetime import datetime
from uuid import uuid4

import aiohttp
import dask.config
import kopf
import kr8s
from distributed.core import clean_exception, rpc
from distributed.protocol.pickle import dumps
from importlib_metadata import entry_points
from kr8s.asyncio.objects import Deployment, Pod, Service

from dask_kubernetes.operator._objects import (
    DaskAutoscaler,
    DaskCluster,
    DaskJob,
    DaskWorkerGroup,
)
from dask_kubernetes.operator.networking import get_scheduler_address

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

    # Create scheduler deployment
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
    scheduler_deployment = await Deployment(data, namespace=namespace)
    if not await scheduler_deployment.exists():
        await scheduler_deployment.create()
    logger.info(
        f"Scheduler deployment {scheduler_deployment.name} created in {namespace}."
    )

    # Create scheduler service
    data = build_scheduler_service_spec(
        name, scheduler_spec.get("service"), annotations, labels
    )
    kopf.adopt(data)
    scheduler_service = await Service(data, namespace=namespace)
    if not await scheduler_service.exists():
        await scheduler_service.create()
    logger.info(f"Scheduler service {data['metadata']['name']} created in {namespace}.")

    # Create default worker group
    worker_spec = spec.get("worker", {})
    annotations = _get_annotations(meta)
    labels = _get_labels(meta)
    if "metadata" in worker_spec:
        if "annotations" in worker_spec["metadata"]:
            annotations.update(**worker_spec["metadata"]["annotations"])
        if "labels" in worker_spec["metadata"]:
            labels.update(**worker_spec["metadata"]["labels"])
    data = build_default_worker_group_spec(name, worker_spec, annotations, labels)
    worker_group = await DaskWorkerGroup(data, namespace=namespace)
    if not await worker_group.exists():
        await worker_group.create()
    logger.info(f"Worker group {data['metadata']['name']} created in {namespace}.")

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
async def daskworkergroup_create(body, namespace, logger, **kwargs):
    wg = await DaskWorkerGroup(body, namespace=namespace)
    cluster = await wg.cluster()
    await cluster.adopt(wg)
    logger.info(f"Successfully adopted by {cluster.name}")

    del kwargs["new"]
    await daskworkergroup_replica_update(
        body=body,
        logger=logger,
        new=wg.replicas,
        namespace=namespace,
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
    logger.warning(
        f"Scaling {worker_group_name} failed via the HTTP API and the Dask RPC, falling back to LIFO scaling. "
        "This can result in lost data, see https://kubernetes.dask.org/en/latest/operator_troubleshooting.html."
    )
    workers = await kr8s.asyncio.get(
        "pods",
        namespace=namespace,
        label_selector={"dask.org/workergroup-name": worker_group_name},
    )
    return [w.name for w in workers[:-n_workers]]


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
    name, namespace, old, new, **kwargs
):
    if old is not None:
        wg = await DaskWorkerGroup.get(f"{name}-default", namespace=namespace)
        await wg.scale(new)


@kopf.on.field("daskworkergroup.kubernetes.dask.org", field="spec.worker.replicas")
async def daskworkergroup_replica_update(
    name, namespace, meta, spec, new, body, logger, **kwargs
):
    cluster_name = spec["cluster"]
    wg = await DaskWorkerGroup(body, namespace=namespace)
    try:
        cluster = await wg.cluster()
    except kr8s.NotFoundError:
        # No need to scale if cluster is deleted, pods will be cleaned up
        return

    # Replica updates can come in quick succession and the changes must be applied atomically to ensure
    # the number of workers ends in the correct state
    async with worker_group_scale_locks[f"{namespace}/{name}"]:
        current_workers = len(
            await kr8s.asyncio.get(
                "deployments",
                namespace=namespace,
                label_selector={"dask.org/workergroup-name": name},
            )
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

        batch_size = int(
            dask.config.get("kubernetes.controller.worker-allocation.batch-size") or 0
        )
        batch_size = min(workers_needed, batch_size) if batch_size else workers_needed
        batch_delay = int(
            dask.config.get("kubernetes.controller.worker-allocation.delay") or 0
        )
        if workers_needed > 0:
            for _ in range(batch_size):
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
                kopf.label(data, labels=cluster.labels)
                worker_deployment = await Deployment(data, namespace=namespace)
                await worker_deployment.create()
        if workers_needed > batch_size:
            raise kopf.TemporaryError(
                "Added maximum number of workers for this batch but still need to create more workers, "
                f"waiting for {batch_delay} seconds before continuing.",
                delay=batch_delay,
            )
        logger.info(f"Scaled worker group {name} up to {desired_workers} workers.")
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
                worker_deployment = await Deployment(wid, namespace=namespace)
                await worker_deployment.delete()
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
    cluster = await DaskCluster(cluster_spec, namespace=namespace)
    await cluster.create()
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
    job_pod = await Pod(job_pod_spec, namespace=namespace)
    await job_pod.create()
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
    name = meta["labels"]["dask.org/cluster-name"]
    job = await DaskJob.get(name, namespace=namespace)
    await job.patch(
        {
            "status": {
                "jobStatus": "Running",
                "startTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
            }
        },
        subresource="status",
    )


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Succeeded",
)
async def handle_runner_status_change_succeeded(meta, namespace, logger, **kwargs):
    logger.info("Job succeeded, deleting Dask cluster.")
    name = meta["labels"]["dask.org/cluster-name"]
    cluster = await DaskCluster.get(name, namespace=namespace)
    await cluster.delete()
    job = await DaskJob.get(name, namespace=namespace)
    await job.patch(
        {
            "status": {
                "jobStatus": "Successful",
                "endTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
            }
        },
        subresource="status",
    )


@kopf.on.field(
    "pod",
    field="status.phase",
    labels={"dask.org/component": "job-runner"},
    new="Failed",
)
async def handle_runner_status_change_succeeded(meta, namespace, logger, **kwargs):
    logger.info("Job failed, deleting Dask cluster.")
    name = meta["labels"]["dask.org/cluster-name"]
    cluster = await DaskCluster.get(name, namespace=namespace)
    await cluster.delete()
    job = await DaskJob.get(name, namespace=namespace)
    await job.patch(
        {
            "status": {
                "jobStatus": "Failed",
                "endTime": datetime.utcnow().strftime(KUBERNETES_DATETIME_FORMAT),
            }
        },
        subresource="status",
    )


@kopf.on.create("daskautoscaler.kubernetes.dask.org")
async def daskautoscaler_create(body, logger, **_):
    """When an autoscaler is created make it a child of the associated cluster for cascade deletion."""
    autoscaler = await DaskAutoscaler(body)
    cluster = await autoscaler.cluster()
    await cluster.adopt(autoscaler)
    logger.info(f"Autoscaler {autoscaler.name} adopted by cluster {cluster.name}")


@kopf.timer("daskautoscaler.kubernetes.dask.org", interval=5.0)
async def daskautoscaler_adapt(spec, name, namespace, logger, **kwargs):
    try:
        scheduler = await Pod.get(
            label_selector={
                "dask.org/component": "scheduler",
                "dask.org/cluster-name": spec["cluster"],
            },
            namespace=namespace,
        )
        if not await scheduler.ready():
            raise ValueError()
    except ValueError:
        logger.info("Scheduler not ready, skipping autoscaling")
        return

    autoscaler = await DaskAutoscaler.get(name, namespace=namespace)
    worker_group = await DaskWorkerGroup.get(
        f"{spec['cluster']}-default", namespace=namespace
    )

    current_replicas = worker_group.replicas
    cooldown_until = float(
        autoscaler.annotations.get(
            DASK_AUTOSCALER_COOLDOWN_UNTIL_ANNOTATION, time.time()
        )
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
        await worker_group.scale(desired_workers)

        cooldown_until = time.time() + 15

        await autoscaler.annotate(
            {DASK_AUTOSCALER_COOLDOWN_UNTIL_ANNOTATION: str(cooldown_until)}
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
        except Exception:
            logger.warn("Unable to connect to scheduler, skipping autoshutdown check.")
            return
        if idle_since and time.time() > idle_since + spec["idleTimeout"]:
            cluster = await DaskCluster.get(name, namespace=namespace)
            await cluster.delete()
