import asyncio
from contextlib import suppress
import random
import socket
import subprocess
import time
from weakref import finalize
import kubernetes_asyncio as kubernetes
from tornado.iostream import StreamClosedError

from distributed.core import rpc

from dask_kubernetes.common.utils import check_dependency
from dask_kubernetes.aiopykube.objects import Pod
from dask_kubernetes.exceptions import CrashLoopBackOffError


async def get_internal_address_for_scheduler_service(
    service,
    port_forward_cluster_ip=None,
    service_name_resolution_retries=20,
    port_name="tcp-comm",
    local_port=None,
):
    """Take a service object and return the scheduler address."""
    [port] = [
        port.port
        for port in service.spec.ports
        if port.name == service.metadata.name or port.name == port_name
    ]
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
    core_api,
    service,
    port_forward_cluster_ip=None,
    service_name_resolution_retries=20,
    port_name="tcp-comm",
    local_port=None,
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
    check_dependency("kubectl")
    if not local_port:
        local_port = _random_free_port(49152, 65535)  # IANA suggested range
    elif _port_in_use(local_port):
        raise ConnectionError("Specified Port already in use.")
    kproc = subprocess.Popen(
        [
            "kubectl",
            "port-forward",
            "--address",
            "0.0.0.0",
            "--namespace",
            f"{namespace}",
            f"service/{service_name}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    finalize(kproc, kproc.kill)

    if await is_comm_open("localhost", local_port, retries=2000):
        return local_port
    raise ConnectionError("kubectl port forward failed")


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
    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        service = await api.read_namespaced_service(service_name, namespace)
        if allow_external:
            address = await get_external_address_for_scheduler_service(
                api,
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


async def wait_for_scheduler(api, cluster_name, namespace, timeout=None):
    pod_start_time = None
    while True:
        async with kubernetes.client.api_client.ApiClient() as api_client:
            k8s_api = kubernetes.client.CoreV1Api(api_client)
            pods = await k8s_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=f"dask.org/component=scheduler,dask.org/cluster-name={cluster_name}",
            )
        pod = await Pod.objects(api, namespace=namespace).get_by_name(
            pods.items[0].metadata.name
        )
        phase = pod.obj["status"]["phase"]
        if phase == "Running":
            if not pod_start_time:
                pod_start_time = time.time()
            conditions = {
                c["type"]: c["status"] for c in pod.obj["status"]["conditions"]
            }
            if "Ready" in conditions and conditions["Ready"] == "True":
                return
            if "containerStatuses" in pod.obj["status"]:
                for container in pod.obj["status"]["containerStatuses"]:
                    if (
                        "waiting" in container["state"]
                        and container["state"]["waiting"]["reason"]
                        == "CrashLoopBackOff"
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
