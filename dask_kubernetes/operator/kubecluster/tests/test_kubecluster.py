import pytest
from dask.distributed import Client
from distributed.utils import TimeoutError

from dask_kubernetes.constants import MAX_CLUSTER_NAME_LEN
from dask_kubernetes.exceptions import SchedulerStartupError, ValidationError
from dask_kubernetes.operator import KubeCluster, make_cluster_spec


def test_kubecluster(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="cluster", namespace=namespace, image=docker_image, n_workers=1
        ) as cluster:
            with Client(cluster) as client:
                client.scheduler_info()
                assert client.submit(lambda x: x + 1, 10).result() == 11


@pytest.mark.anyio
async def test_kubecluster_async(kopf_runner, docker_image, namespace):
    with kopf_runner:
        async with KubeCluster(
            name="async",
            namespace=namespace,
            image=docker_image,
            n_workers=1,
            asynchronous=True,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                assert await client.submit(lambda x: x + 1, 10).result() == 11


def test_custom_worker_command(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="customworker",
            image=docker_image,
            worker_command=["python", "-m", "distributed.cli.dask_worker"],
            n_workers=1,
            namespace=namespace,
        ) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="bar", image=docker_image, n_workers=1, namespace=namespace
        ) as cluster1:
            with Client(cluster1) as client1:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
        with KubeCluster(
            name="baz", image=docker_image, n_workers=1, namespace=namespace
        ) as cluster2:
            with Client(cluster2) as client2:
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_clusters_with_custom_port_forward(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="bar",
            image=docker_image,
            n_workers=1,
            scheduler_forward_port=8888,
            namespace=namespace,
        ) as cluster1:
            assert cluster1.forwarded_dashboard_port == "8888"
            with Client(cluster1) as client1:
                assert client1.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters_simultaneously(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with (
            KubeCluster(
                name="fizz", image=docker_image, n_workers=1, namespace=namespace
            ) as cluster1,
            KubeCluster(
                name="buzz", image=docker_image, n_workers=1, namespace=namespace
            ) as cluster2,
        ):
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters_simultaneously_same_loop(
    kopf_runner, docker_image, namespace
):
    with kopf_runner:
        with (
            KubeCluster(
                name="fizz", image=docker_image, n_workers=1, namespace=namespace
            ) as cluster1,
            KubeCluster(
                name="buzz",
                image=docker_image,
                loop=cluster1.loop,
                n_workers=1,
                namespace=namespace,
            ) as cluster2,
        ):
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert cluster1.loop is cluster2.loop is client1.loop is client2.loop
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


@pytest.mark.anyio
async def test_cluster_from_name(kopf_runner, docker_image, namespace):
    with kopf_runner:
        async with KubeCluster(
            name="abc",
            namespace=namespace,
            image=docker_image,
            n_workers=1,
            asynchronous=True,
        ) as firstcluster:
            async with KubeCluster.from_name(
                "abc", namespace=namespace, asynchronous=True
            ) as secondcluster:
                assert firstcluster == secondcluster
                firstcluster.scale(2)
                assert firstcluster.scheduler_info == secondcluster.scheduler_info


def test_additional_worker_groups(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="additionalgroups",
            n_workers=1,
            image=docker_image,
            namespace=namespace,
        ) as cluster:
            cluster.add_worker_group(name="more", n_workers=1)
            with Client(cluster) as client:
                client.wait_for_workers(2)
                assert client.submit(lambda x: x + 1, 10).result() == 11
            cluster.delete_worker_group(name="more")


def test_cluster_without_operator(docker_image, namespace):
    with pytest.raises(TimeoutError, match="is the Dask Operator running"):
        KubeCluster(
            name="noop",
            n_workers=1,
            image=docker_image,
            resource_timeout=1,
            namespace=namespace,
        )


def test_cluster_crashloopbackoff(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with pytest.raises(SchedulerStartupError, match="Scheduler failed to start"):
            spec = make_cluster_spec(name="crashloopbackoff", n_workers=1)
            spec["spec"]["scheduler"]["spec"]["containers"][0]["args"][
                0
            ] = "dask-schmeduler"
            KubeCluster(
                custom_cluster_spec=spec,
                namespace=namespace,
                resource_timeout=1,
                idle_timeout=2,
            )


def test_adapt(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="adaptive",
            image=docker_image,
            n_workers=0,
            namespace=namespace,
        ) as cluster:
            cluster.adapt(minimum=0, maximum=1)
            with Client(cluster) as client:
                f = client.submit(lambda x: x + 1, 10)
                assert f.result() == 11

            # Need to clean up the DaskAutoscaler object
            # See https://github.com/dask/dask-kubernetes/issues/546
            cluster.scale(0)


def test_custom_spec(kopf_runner, docker_image, namespace):
    with kopf_runner:
        spec = make_cluster_spec("customspec", image=docker_image)
        with KubeCluster(
            custom_cluster_spec=spec, n_workers=1, namespace=namespace
        ) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11


def test_typo_resource_limits(namespace):
    with pytest.raises(ValueError):
        KubeCluster(
            name="foo",
            resources={
                "limit": {  # <-- Typo, should be `limits`
                    "CPU": "1",
                },
            },
            namespace=namespace,
        )


@pytest.mark.parametrize(
    "cluster_name",
    [
        (MAX_CLUSTER_NAME_LEN + 1) * "a",
        "invalid.chars.in.name",
    ],
)
def test_invalid_cluster_name_fails(cluster_name, kopf_runner, docker_image, namespace):
    with kopf_runner:
        with pytest.raises(ValidationError):
            KubeCluster(
                name=cluster_name,
                namespace=namespace,
                image=docker_image,
            )
