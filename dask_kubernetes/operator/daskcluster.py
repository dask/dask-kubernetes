import asyncio

from distributed.core import rpc

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4


def build_scheduler_pod_spec(name, image):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-scheduler",
            "labels": {
                "dask.org/cluster-name": name,
                "dask.org/component": "scheduler",
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": ["dask-scheduler"],
                    "ports": [
                        {
                            "name": "comm",
                            "containerPort": 8786,
                            "protocol": "TCP",
                        },
                        {
                            "name": "dashboard",
                            "containerPort": 8787,
                            "protocol": "TCP",
                        },
                    ],
                    "readinessProbe": {
                        "tcpSocket": {"port": "comm"},
                        "initialDelaySeconds": 5,
                        "periodSeconds": 10,
                    },
                    "livenessProbe": {
                        "tcpSocket": {"port": "comm"},
                        "initialDelaySeconds": 15,
                        "periodSeconds": 20,
                    },
                }
            ]
        },
    }


def build_scheduler_service_spec(name):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {
            "name": name,
            "labels": {
                "dask.org/cluster-name": name,
            },
        },
        "spec": {
            "selector": {
                "dask.org/cluster-name": name,
                "dask.org/component": "scheduler",
            },
            "ports": [
                {
                    "name": "comm",
                    "protocol": "TCP",
                    "port": 8786,
                    "targetPort": 8786,
                },
                {
                    "name": "dashboard",
                    "protocol": "TCP",
                    "port": 8787,
                    "targetPort": 8787,
                },
            ],
        },
    }


def build_worker_pod_spec(name, namespace, image, n, scheduler_name):
    worker_name = f"{scheduler_name}-{name}-worker-{n}"
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": worker_name,
            "labels": {
                "dask.org/cluster-name": scheduler_name,
                "dask.org/workergroup-name": name,
                "dask.org/component": "worker",
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": [
                        "dask-worker",
                        f"tcp://{scheduler_name}.{namespace}:8786",
                        f"--name={worker_name}",
                    ],
                }
            ]
        },
    }


def build_worker_group_spec(name, image, replicas, resources, env):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskWorkerGroup",
        "metadata": {"name": f"{name}-worker-group"},
        "spec": {
            "image": image,
            "replicas": replicas,
            "resources": resources,
            "env": env,
        },
    }


def build_cluster_spec(name, image, replicas, resources, env):
    return {
        "apiVersion": "kubernetes.dask.org/v1",
        "kind": "DaskCluster",
        "metadata": {"name": f"{name}-cluster"},
        "spec": {
            "image": image,
            "scheduler": {"serviceType": "ClusterIP"},
            "replicas": replicas,
            "resources": resources,
            "env": env,
        },
    }


async def wait_for_scheduler(cluster_name, namespace):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        watch = kubernetes.watch.Watch()
        async for event in watch.stream(
            func=api.list_namespaced_pod,
            namespace=namespace,
            label_selector=f"dask.org/cluster-name={cluster_name},dask.org/component=scheduler",
            timeout_seconds=60,
        ):
            if event["object"].status.phase == "Running":
                watch.stop()
            await asyncio.sleep(0.1)


@kopf.on.create("daskcluster")
async def daskcluster_create(spec, name, namespace, logger, **kwargs):
    await kubernetes.config.load_kube_config()
    logger.info(
        f"A DaskCluster has been created called {name} in {namespace} with the following config: {spec}"
    )
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        # TODO Check for existing scheduler pod
        data = build_scheduler_pod_spec(name, spec.get("image"))
        kopf.adopt(data)
        scheduler_pod = await api.create_namespaced_pod(
            namespace=namespace,
            body=data,
        )
        logger.info(
            f"A scheduler pod has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )

        # TODO Check for existing scheduler service
        data = build_scheduler_service_spec(name)
        kopf.adopt(data)
        scheduler_service = await api.create_namespaced_service(
            namespace=namespace,
            body=data,
        )
        logger.info(
            f"A scheduler service has been created called {data['metadata']['name']} in {namespace} \
            with the following config: {data['spec']}"
        )

        data = build_worker_group_spec(
            "default",
            spec.get("image"),
            spec.get("replicas"),
            spec.get("resources"),
            spec.get("env"),
        )
        # TODO: Next line is not needed if we can get worker groups adopted by the cluster
        kopf.adopt(data)
        api = kubernetes.client.CustomObjectsApi(api_client)
        worker_pods = await api.create_namespaced_custom_object(
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
        logger.info(
            "______________________________________________________________________"
        )


@kopf.on.create("daskworkergroup")
async def daskworkergroup_create(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        cluster = await kubernetes.client.CustomObjectsApi(
            api_client
        ).list_cluster_custom_object(
            group="kubernetes.dask.org", version="v1", plural="daskclusters"
        )
        scheduler_name = cluster["items"][0]["metadata"]["name"]
        num_workers = spec["replicas"]
        for i in range(num_workers):
            data = build_worker_pod_spec(
                name, namespace, spec.get("image"), uuid4().hex, scheduler_name
            )
            kopf.adopt(data)
            worker_pod = await api.create_namespaced_pod(
                namespace=namespace,
                body=data,
            )
        logger.info(f"{num_workers} Worker pods in created in {namespace}")


@kopf.on.update("daskworkergroup")
async def daskworkergroup_update(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)

        scheduler = await kubernetes.client.CustomObjectsApi(
            api_client
        ).list_cluster_custom_object(
            group="kubernetes.dask.org", version="v1", plural="daskclusters"
        )
        scheduler_name = scheduler["items"][0]["metadata"]["name"]
        workers = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=f"dask.org/workergroup-name={name}",
        )
        current_workers = len(workers.items)
        desired_workers = spec["replicas"]
        workers_needed = desired_workers - current_workers
        if workers_needed > 0:
            for i in range(workers_needed):
                data = build_worker_pod_spec(
                    name, namespace, spec.get("image"), uuid4().hex, scheduler_name
                )
                kopf.adopt(data)
                worker_pod = await api.create_namespaced_pod(
                    namespace=namespace,
                    body=data,
                )
            logger.info(f"Scaled worker group {name} up to {spec['replicas']} workers.")
        if workers_needed < 0:
            # TODO: Replace address localhost with the scheduler service name
            scheduler = rpc("localhost:8786")
            worker_ids = await scheduler.workers_to_close(
                n=-workers_needed, attribute="name"
            )
            logger.info(f"Workers to close: {worker_ids}")
            for wid in worker_ids:
                worker_pod = await api.delete_namespaced_pod(
                    name=wid,
                    namespace=namespace,
                )
            logger.info(
                f"Scaled worker group {name} down to {spec['replicas']} workers."
            )


@kopf.on.delete("daskcluster")
async def daskcluster_delete(spec, name, namespace, logger, **kwargs):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CustomObjectsApi(api_client)
        workergroups = await api.list_cluster_custom_object(
            group="kubernetes.dask.org", version="v1", plural="daskworkergroups"
        )
        workergroups = await api.delete_collection_namespaced_custom_object(
            group="kubernetes.dask.org",
            version="v1",
            plural="daskworkergroups",
            namespace=namespace,
        )
        # TODO: We would prefer to use adoptions rather than a delete handler
