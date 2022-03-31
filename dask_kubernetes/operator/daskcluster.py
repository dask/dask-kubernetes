import asyncio

import subprocess

# import json

from distributed.core import rpc

import kopf
import kubernetes_asyncio as kubernetes

from uuid import uuid4

"""Utility functions."""
import os
import random
import shutil
import socket
import subprocess
import string
import time
from weakref import finalize

from dask.distributed import Client


def format_labels(labels):
    """Convert a dictionary of labels into a comma separated string"""
    if labels:
        return ",".join(["{}={}".format(k, v) for k, v in labels.items()])
    else:
        return ""


def escape(s):
    valid_characters = string.ascii_letters + string.digits + "-"
    return "".join(c for c in s if c in valid_characters).lower()


def namespace_default():
    """
    Get current namespace if running in a k8s cluster
    If not in a k8s cluster with service accounts enabled, default to
    'default'
    Taken from https://github.com/jupyterhub/kubespawner/blob/master/kubespawner/spawner.py#L125
    """
    ns_path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return "default"


async def get_external_address_for_scheduler_service(
    core_api, service, port_forward_cluster_ip=None, service_name_resolution_retries=20
):
    """Take a service object and return the scheduler address."""
    [port] = [
        port.port
        for port in service.spec.ports
        if port.name == service.metadata.name or port.name == "comm"
    ]
    if service.spec.type == "LoadBalancer":
        lb = service.status.load_balancer.ingress[0]
        host = lb.hostname or lb.ip
    elif service.spec.type == "NodePort":
        nodes = await core_api.list_node()
        host = nodes.items[0].status.addresses[0].address
    elif service.spec.type == "ClusterIP":
        try:
            # Try to resolve the service name. If we are inside the cluster this should succeed.
            host = f"{service.metadata.name}.{service.metadata.namespace}"
            _is_service_available(
                host=host, port=port, retries=service_name_resolution_retries
            )
        except socket.gaierror:
            # If we are outside it will fail and we need to port forward the service.
            host = "localhost"
            port = await port_forward_service(
                service.metadata.name, service.metadata.namespace, port
            )
    return f"tcp://{host}:{port}"


def _is_service_available(host, port, retries=20):
    for i in range(retries):
        try:
            return socket.getaddrinfo(host, port)
        except socket.gaierror as e:
            if i >= retries - 1:
                raise e
            time.sleep(0.5)


def _random_free_port(low, high, retries=20):
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while retries:
        guess = random.randint(low, high)
        try:
            conn.bind(("", guess))
            conn.close()
            return guess
        except OSError:
            retries -= 1
    raise ConnectionError("Not able to find a free port.")


async def port_forward_service(service_name, namespace, remote_port, local_port=None):
    check_dependency("kubectl")
    if not local_port:
        local_port = _random_free_port(49152, 65535)  # IANA suggested range
    kproc = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            "--namespace",
            f"{namespace}",
            f"service/{service_name}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    finalize(kproc, kproc.kill)

    if await is_comm_open("localhost", local_port, retries=100):
        return local_port
    raise ConnectionError("kubectl port forward failed")


async def is_comm_open(ip, port, retries=10):
    while retries > 0:
        try:
            async with Client(f"tcp://{ip}:{port}", asynchronous=True, timeout=2):
                return True
        except Exception:
            time.sleep(0.5)
            retries -= 1
    return False


def check_dependency(dependency):
    if shutil.which(dependency) is None:
        raise RuntimeError(
            f"Missing dependency {dependency}. "
            f"Please install {dependency} following the instructions for your OS. "
        )


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
            service_name = "foo-cluster"
            service = await api.read_namespaced_service(service_name, namespace)
            port_forward_cluster_ip = None
            address = await get_external_address_for_scheduler_service(
                api, service, port_forward_cluster_ip=port_forward_cluster_ip
            )
            # scheduler = rpc("localhost:8786")
            scheduler = rpc(address)
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
            scheduler.close_comms()
            scheduler.close_rpc()


# @kopf.on.delete("daskcluster")
# async def daskcluster_delete(spec, name, namespace, logger, **kwargs):
#     patch = {"metadata": {"finalizers": []}}
#     json_patch = json.dumps(patch)
#     subprocess.check_output(
#         [
#             "kubectl",
#             "patch",
#             "daskcluster",
#             f"{name}",
#             "--patch",
#             str(json_patch),
#             "--type=merge",
#         ],
#         encoding="utf-8",
#     )
#     await kubernetes.config.load_kube_config()
#     async with kubernetes.client.api_client.ApiClient() as api_client:
#         api = kubernetes.client.CustomObjectsApi(api_client)
#         workergroups = await api.list_cluster_custom_object(
#             group="kubernetes.dask.org", version="v1", plural="daskworkergroups"
#         )
#         # TODO: Delete worker groups in workergroups
#         workergroups = api.delete_namespaced_custom_object(
#             group="kubernetes.dask.org",
#             version="v1",
#             plural="daskworkergroups",
#             namespace=namespace,
#             name=name,
#             async_req=True,
#         )
#         # TODO: We would prefer to use adoptions rather than a delete handler
