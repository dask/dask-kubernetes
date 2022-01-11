import asyncio

import kopf
import kubernetes


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


def build_worker_pod_spec(name, namespace, image, n):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"{name}-worker-{n}",
            "labels": {
                "dask.org/cluster-name": name,
                "dask.org/component": "worker",
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "scheduler",
                    "image": image,
                    "args": ["dask-worker", f"tcp://{name}.{namespace}:8786"],
                }
            ]
        },
    }


async def wait_for_scheduler(cluster_name, namespace):
    api = kubernetes.client.CoreV1Api()
    watch = kubernetes.watch.Watch()
    for event in watch.stream(
        func=api.list_namespaced_pod,
        namespace=namespace,
        label_selector=f"dask.org/cluster-name={cluster_name},dask.org/component=scheduler",
        timeout_seconds=60,
    ):
        if event["object"].status.phase == "Running":
            watch.stop()
        await asyncio.sleep(0.1)


@kopf.on.create("daskclusters")
async def create_cluster(spec, name, namespace, logger, **kwargs):
    api = kubernetes.client.CoreV1Api()

    # TODO Check for existing scheduler pod
    data = build_scheduler_pod_spec(name, spec.get("image"))
    kopf.adopt(data)
    scheduler_pod = api.create_namespaced_pod(
        namespace=namespace,
        body=data,
    )
    # await wait_for_scheduler(name, namespace)
    logger.info(f"Scheduler pod is created")

    # TODO Check for existing scheduler service
    data = build_scheduler_service_spec(name)
    kopf.adopt(data)
    scheduler_pod = api.create_namespaced_service(
        namespace=namespace,
        body=data,
    )
    logger.info(f"Scheduler service is created")


@kopf.timer("daskclusters", interval=5.0)
async def scale_workers(spec, name, namespace, logger, **kwargs):
    # TODO Connect to cluster RPC for asking which workers to scale down
    api = kubernetes.client.CoreV1Api()

    # Wait for scheduler service
    while True:
        services = api.list_namespaced_service(
            namespace=namespace,
            label_selector=f"dask.org/cluster-name={name}",
        )
        if len(services.items) > 0:
            break
        else:
            await asyncio.sleep(1)

    workers = api.list_namespaced_pod(
        namespace=namespace,
        label_selector=f"dask.org/cluster-name={name},dask.org/component=worker",
    )
    current_workers = len(workers.items)
    desired_workers = spec["workers"]["replicas"]
    workers_needed = desired_workers - current_workers
    if workers_needed > 0:
        for i in range(current_workers, current_workers + workers_needed):
            data = build_worker_pod_spec(name, namespace, spec.get("image"), i)
            kopf.adopt(data)
            worker_pod = api.create_namespaced_pod(
                namespace=namespace,
                body=data,
            )
    if workers_needed < 0:
        pass
        # TODO Scale down

    logger.info(f"Scaled cluster to {desired_workers} workers.")