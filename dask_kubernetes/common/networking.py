import asyncio
import random
import socket
import subprocess
import time
from weakref import finalize

import kubernetes_asyncio as kubernetes
from tornado.iostream import StreamClosedError

from distributed.core import rpc

from .utils import check_dependency


async def get_external_address_for_scheduler_service(
    core_api,
    service,
    port_forward_cluster_ip=None,
    service_name_resolution_retries=20,
    port_name="tcp-comm",
):
    """Take a service object and return the scheduler address."""
    [port] = [
        port.port
        for port in service.spec.ports
        if port.name == service.metadata.name or port.name == port_name
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
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while retries > 0:
        result = sock.connect_ex((ip, port))
        if result == 0:
            return True
        else:
            time.sleep(2)
            retries -= 1
    return False


async def port_forward_dashboard(service_name, namespace):
    port = await port_forward_service(service_name, namespace, 8787)
    return port


async def get_scheduler_address(service_name, namespace, port_name="tcp-comm"):
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        service = await api.read_namespaced_service(service_name, namespace)
        port_forward_cluster_ip = None
        address = await get_external_address_for_scheduler_service(
            api,
            service,
            port_forward_cluster_ip=port_forward_cluster_ip,
            port_name=port_name,
        )
        return address


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
            if event["object"].status.conditions:
                conditions = {
                    c.type: c.status for c in event["object"].status.conditions
                }
                if "Ready" in conditions and conditions["Ready"] == "True":
                    watch.stop()
            await asyncio.sleep(0.1)


async def wait_for_scheduler_comm(address):
    while True:
        try:
            async with rpc(address) as scheduler_comm:
                await scheduler_comm.versions()
        except (StreamClosedError, OSError):
            await asyncio.sleep(0.1)
            continue
        break
