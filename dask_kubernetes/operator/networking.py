import asyncio
import random
import socket
import threading
import time
from contextlib import suppress

import kr8s
from distributed.core import rpc
from kr8s.asyncio.objects import Pod, Service
from tornado.iostream import StreamClosedError

from dask_kubernetes.exceptions import CrashLoopBackOffError


async def get_internal_address_for_scheduler_service(
    service,
    port_forward_cluster_ip=None,
    service_name_resolution_retries=20,
    port_name="tcp-comm",
    local_port=None,
):
    """Take a service object and return the scheduler address."""
    port = _get_port(service, port_name)
    if not port_forward_cluster_ip:
        with suppress(socket.gaierror):
            # Try to resolve the service name. If we are inside the cluster this should succeed.
            host = f"{service.metadata.name}.{service.metadata.namespace}"
            if await _is_service_available(
                host=host, port=port, retries=service_name_resolution_retries
            ):
                return f"tcp://{host}:{port}"

    # If the service name is unresolvable, we are outside the cluster and we need to port forward the service.
    host = "localhost"

    port = await port_forward_service(
        service.metadata.name, service.metadata.namespace, port, local_port
    )
    return f"tcp://{host}:{port}"


async def get_external_address_for_scheduler_service(
    service,
    port_forward_cluster_ip=None,
    service_name_resolution_retries=20,
    port_name="tcp-comm",
    local_port=None,
):
    """Take a service object and return the scheduler address."""
    if service.spec.type == "LoadBalancer":
        port = _get_port(service, port_name)
        lb = service.status.loadBalancer.ingress[0]
        host = lb.get("hostname", None) or lb.ip
    elif service.spec.type == "NodePort":
        port = _get_port(service, port_name, is_node_port=True)
        nodes = await kr8s.asyncio.get("nodes")
        host = nodes[0].status.addresses[0].address
    elif service.spec.type == "ClusterIP":
        port = _get_port(service, port_name)
        if not port_forward_cluster_ip:
            with suppress(socket.gaierror):
                # Try to resolve the service name. If we are inside the cluster this should succeed.
                host = f"{service.metadata.name}.{service.metadata.namespace}"
                if await _is_service_available(
                    host=host, port=port, retries=service_name_resolution_retries
                ):
                    return f"tcp://{host}:{port}"

        # If the service name is unresolvable, we are outside the cluster and we need to port forward the service.
        host = "localhost"

        port = await port_forward_service(
            service.metadata.name, service.metadata.namespace, port, local_port
        )
    return f"tcp://{host}:{port}"


def _get_port(service, port_name, is_node_port=False):
    """NodePort is a special case when we have to use node_port instead of node"""
    [port] = [
        port.port if not is_node_port else port.nodePort
        for port in service.spec.ports
        if port.name == service.metadata.name or port.name == port_name
    ]
    return port


async def _is_service_available(host, port, retries=20):
    for i in range(retries):
        try:
            return await asyncio.get_event_loop().getaddrinfo(host, port)
        except socket.gaierror as e:
            if i >= retries - 1:
                raise e
            await asyncio.sleep(0.5)


def _port_in_use(port):
    if port is None:
        return True
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        conn.bind(("", port))
        conn.close()
        return False
    except OSError:
        return True


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
    if not local_port:
        local_port = _random_free_port(49152, 65535)  # IANA suggested range
    elif _port_in_use(local_port):
        raise ConnectionError("Specified Port already in use.")
    pf = threading.Thread(
        name=f"DaskKubernetesPortForward ({namespace}/{service_name} {local_port}->{remote_port})",
        target=run_port_forward,
        args=(
            service_name,
            namespace,
            remote_port,
            local_port,
        ),
        daemon=True,
    )
    pf.start()

    if await is_comm_open("localhost", local_port, retries=2000):
        return local_port
    raise ConnectionError("port forward failed")


def run_port_forward(service_name, namespace, remote_port, local_port):
    async def _run():
        svc = await Service.get(service_name, namespace=namespace)
        async with svc.portforward(remote_port, local_port):
            while True:
                await asyncio.sleep(0.1)

    asyncio.run(_run())


async def is_comm_open(ip, port, retries=200):
    while retries > 0:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            result = sock.connect_ex((ip, port))
        if result == 0:
            return True
        else:
            time.sleep(0.1)
            retries -= 1
    return False


async def port_forward_dashboard(service_name, namespace):
    port = await port_forward_service(service_name, namespace, 8787)
    return port


async def get_scheduler_address(
    service_name,
    namespace,
    port_name="tcp-comm",
    port_forward_cluster_ip=None,
    local_port=None,
    allow_external=True,
):
    service = await Service.get(service_name, namespace=namespace)
    if allow_external:
        address = await get_external_address_for_scheduler_service(
            service,
            port_forward_cluster_ip=port_forward_cluster_ip,
            port_name=port_name,
            local_port=local_port,
        )
    else:
        address = await get_internal_address_for_scheduler_service(
            service,
            port_forward_cluster_ip=port_forward_cluster_ip,
            port_name=port_name,
            local_port=local_port,
        )
    return address


async def wait_for_scheduler(cluster_name, namespace, timeout=None):
    pod_start_time = None
    while True:
        try:
            pod = await Pod.get(
                label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={cluster_name}",
                field_selector="status.phase=Running",
                namespace=namespace,
            )
        except kr8s.NotFoundError:
            await asyncio.sleep(0.25)
            continue
        if pod.status.phase == "Running":
            if not pod_start_time:
                pod_start_time = time.time()
            if await pod.ready():
                return
            if "containerStatuses" in pod.status:
                for container in pod.status.containerStatuses:
                    if (
                        "waiting" in container.state
                        and container.state.waiting.reason == "CrashLoopBackOff"
                        and timeout
                        and pod_start_time + timeout < time.time()
                    ):
                        raise CrashLoopBackOffError(
                            f"Scheduler in CrashLoopBackOff for more than {timeout} seconds."
                        )
        await asyncio.sleep(0.25)


async def wait_for_scheduler_comm(address):
    while True:
        try:
            async with rpc(address) as scheduler_comm:
                await scheduler_comm.versions()
        except (StreamClosedError, OSError):
            await asyncio.sleep(0.1)
            continue
        break
