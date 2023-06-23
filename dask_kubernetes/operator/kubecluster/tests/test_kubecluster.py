import pytest
import kubernetes_asyncio as kubernetes

from dask.distributed import Client
from distributed.utils import TimeoutError

from dask_kubernetes.operator._objects import DaskCluster
from dask_kubernetes.operator import KubeCluster, make_cluster_spec
from dask_kubernetes.exceptions import SchedulerStartupError


def test_experimental_shim():
    with pytest.deprecated_call():
        from dask_kubernetes.experimental import KubeCluster as ExperimentalKubeCluster

    assert ExperimentalKubeCluster is KubeCluster


def test_kubecluster(cluster):
    assert "foo" in cluster.name

    with Client(cluster) as client:
        client.scheduler_info()
        cluster.scale(1)
        assert client.submit(lambda x: x + 1, 10).result() == 11


@pytest.mark.asyncio
async def test_kubecluster_async(kopf_runner, docker_image):
    with kopf_runner:
        async with KubeCluster(
            name="async",
            image=docker_image,
            n_workers=1,
            asynchronous=True,
        ) as cluster:
            async with Client(cluster, asynchronous=True) as client:
                assert await client.submit(lambda x: x + 1, 10).result() == 11


def test_custom_worker_command(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="customworker",
            image=docker_image,
            worker_command=["python", "-m", "distributed.cli.dask_worker"],
            n_workers=1,
        ) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="bar", image=docker_image, n_workers=1) as cluster1:
            with Client(cluster1) as client1:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
        with KubeCluster(name="baz", image=docker_image, n_workers=1) as cluster2:
            with Client(cluster2) as client2:
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_clusters_with_custom_port_forward(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="bar", image=docker_image, n_workers=1, scheduler_forward_port=8888
        ) as cluster1:
            assert cluster1.forwarded_dashboard_port == "8888"
            with Client(cluster1) as client1:
                assert client1.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters_simultaneously(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="fizz", image=docker_image, n_workers=1
        ) as cluster1, KubeCluster(
            name="buzz", image=docker_image, n_workers=1
        ) as cluster2:
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


@pytest.mark.skip(reason="Flaky and fails ~10% of the time.")
def test_multiple_clusters_simultaneously_same_loop(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="fizz", image=docker_image, n_workers=1
        ) as cluster1, KubeCluster(
            name="buzz", image=docker_image, loop=cluster1.loop, n_workers=1
        ) as cluster2:
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert cluster1.loop is cluster2.loop is client1.loop is client2.loop
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


@pytest.mark.asyncio
async def test_cluster_from_name(kopf_runner, docker_image, ns):
    with kopf_runner:
        async with KubeCluster(
            name="abc",
            namespace=ns,
            image=docker_image,
            n_workers=1,
            asynchronous=True,
        ) as firstcluster:
            async with KubeCluster.from_name(
                "abc", namespace=ns, asynchronous=True
            ) as secondcluster:
                assert firstcluster == secondcluster
            cluster = await DaskCluster.get("abc", namespace=ns)
            assert cluster.status["phase"] == "Running"


def test_cluster_scheduler_info_updated(kopf_runner, docker_image, ns):
    with kopf_runner:
        with KubeCluster(name="abc", namespace=ns, image=docker_image) as firstcluster:
            with KubeCluster.from_name("abc", namespace=ns) as secondcluster:
                firstcluster.scale(1)
                assert firstcluster.scheduler_info == secondcluster.scheduler_info


def test_additional_worker_groups(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="additionalgroups", n_workers=1, image=docker_image
        ) as cluster:
            cluster.add_worker_group(name="more", n_workers=1)
            with Client(cluster) as client:
                client.wait_for_workers(2)
                assert client.submit(lambda x: x + 1, 10).result() == 11
            cluster.delete_worker_group(name="more")


def test_cluster_without_operator(docker_image):
    with pytest.raises(TimeoutError, match="is the Dask Operator running"):
        KubeCluster(name="noop", n_workers=1, image=docker_image, resource_timeout=1)


def test_cluster_crashloopbackoff(kopf_runner, docker_image):
    with kopf_runner:
        with pytest.raises(SchedulerStartupError, match="Scheduler failed to start"):
            spec = make_cluster_spec(name="crashloopbackoff", n_workers=1)
            spec["spec"]["scheduler"]["spec"]["containers"][0]["args"][
                0
            ] = "dask-schmeduler"
            KubeCluster(custom_cluster_spec=spec, resource_timeout=1, idle_timeout=2)


def test_adapt(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="adaptive",
            image=docker_image,
            n_workers=0,
        ) as cluster:
            cluster.adapt(minimum=0, maximum=1)
            with Client(cluster) as client:
                f = client.submit(lambda x: x + 1, 10)
                assert f.result() == 11

            # Need to clean up the DaskAutoscaler object
            # See https://github.com/dask/dask-kubernetes/issues/546
            cluster.scale(0)


def test_custom_spec(kopf_runner, docker_image):
    with kopf_runner:
        spec = make_cluster_spec("customspec", image=docker_image)
        with KubeCluster(custom_cluster_spec=spec, n_workers=1) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11


def test_typo_resource_limits(kopf_runner):
    with kopf_runner:
        with pytest.raises(ValueError):
            KubeCluster(
                name="foo",
                resources={
                    "limit": {  # <-- Typo, should be `limits`
                        "CPU": "1",
                    },
                },
            )


@pytest.mark.xfail(reason="Intermittently fails in CI")
def test_invalid_kwargs_exception(kopf_runner):
    with kopf_runner:
        with pytest.raises(kubernetes.client.ApiException):
            KubeCluster(name="foo", n_workers="1")
