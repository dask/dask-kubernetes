import pytest

from dask.distributed import Client
from distributed.utils import TimeoutError

from dask_kubernetes.operator import KubeCluster, make_cluster_spec


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


@pytest.mark.skip(
    reason="Failing due to a race condition that I can't get to the bottom of"
)
def test_cluster_from_name(kopf_runner, docker_image, namespace):
    with kopf_runner:
        with KubeCluster(
            name="abc", namespace=namespace, image=docker_image, n_workers=1
        ) as firstcluster:
            with KubeCluster.from_name("abc", namespace=namespace) as secondcluster:
                assert firstcluster == secondcluster


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


def test_adapt(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(
            name="adaptive",
            image=docker_image,
            n_workers=0,
        ) as cluster:
            cluster.adapt(minimum=0, maximum=1)
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11

            # Need to clean up the DaskAutoscaler object
            # See https://github.com/dask/dask-kubernetes/issues/546
            cluster.scale(0)


def test_custom_spec(kopf_runner, docker_image):
    with kopf_runner:
        spec = make_cluster_spec("customspec", image=docker_image)
        with KubeCluster(custom_cluster_spec=spec, n_workers=1) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11
