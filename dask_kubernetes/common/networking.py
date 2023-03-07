import asyncio
import aiohttp
from contextlib import suppress
import random
import socket
import time
import kubernetes_asyncio as kubernetes
from tornado.iostream import StreamClosedError

from distributed.core import rpc

from .utils import check_dependency


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

    async with kubernetes.client.api_client.ApiClient() as api_client:
        api = kubernetes.client.CoreV1Api(api_client)
        service = await api.read_namespaced_service(service_name, namespace)
        pods = await api.list_namespaced_pod(
            namespace=namespace,
            label_selector=",".join(
                [f"{key}={value}" for key, value in service.spec.selector.items()]
            ),
        )
        pod_name = pods.items[0].metadata.name

    # TODO Move this loop into a task that is tied to the KubeCluster object
    # When the cluster is closed this task should be cancelled.
    while True:
        async with kubernetes.stream.WsApiClient() as ws_api_client:
            api = kubernetes.client.CoreV1Api(ws_api_client)
            ws = await api.connect_get_namespaced_pod_portforward(
                pod_name,
                namespace,
                ports=remote_port,
                _preload_content=False,
            )
            port_forward = PortForward(ws, local_port)
            await port_forward.run()

    if await is_comm_open("localhost", local_port, retries=2000):
        return local_port
    raise ConnectionError("kubectl port forward failed")


class ConnectionClosedError(Exception):
    """A connection has been closed."""


class PortForward:
    def __init__(self, websocket, port):
        self.websocket = websocket
        self.port = port
        self.server = None
        self.channels = []

    async def run(self):
        """Start a server on a local port and sync it with the websocket."""
        self.server = await asyncio.start_server(self.sync_sockets, port=self.port)
        async with self.server:
            await self.server.start_serving()
            await self.server.wait_closed()

    async def sync_sockets(self, reader, writer):
        """Start two tasks to copy bytes from tcp=>websocket and websocket=>tcp."""
        try:
            tasks = [
                asyncio.create_task(self.tcp_to_ws(reader)),
                asyncio.create_task(self.ws_to_tcp(writer)),
            ]
            await asyncio.gather(*tasks)
        except ConnectionClosedError:
            for task in tasks:
                task.cancel()
        finally:
            writer.close()
            self.server.close()

    async def tcp_to_ws(self, reader):
        while self.server.is_serving():
            data = await reader.read(1024 * 1024)
            # TODO Figure out why `data` is empty the first time this is called
            # This indicates the socket is cloed, but it isn't and on the second read it works fine.
            if not data:
                raise ConnectionClosedError("TCP socket closed")
            else:
                # Send data to channel 0 of the websocket.
                await self.websocket.send_bytes(b"\x00" + data)

    async def ws_to_tcp(self, writer):
        while self.server.is_serving():
            message = await self.websocket.receive()
            if message.type == aiohttp.WSMsgType.CLOSED:
                # TODO Figure out why this websocket closes after 4 messages.
                raise ConnectionClosedError("Websocket closed")
            elif message.type == aiohttp.WSMsgType.BINARY:
                # Kubernetes portforward protocol prefixes all frames with a byte to represent
                # the channel. Channel 0 is rw for data and channel 1 is ro for errors.
                if message.data[0] not in self.channels:
                    # Keep track of our channels. Could be useful later for listening to multiple ports.
                    self.channels.append(message.data[0])
                else:
                    writer.write(message.data[1:])
                    await writer.drain()


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
