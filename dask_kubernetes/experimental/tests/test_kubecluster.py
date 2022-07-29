import pytest

from dask.distributed import Client

from dask_kubernetes.experimental import KubeCluster


@pytest.fixture
def cluster(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="foo", image=docker_image) as cluster:
            yield cluster


def test_kubecluster(cluster):
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
        ) as cluster:
            with Client(cluster) as client:
                assert client.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="bar", image=docker_image) as cluster1:
            with Client(cluster1) as client1:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
        with KubeCluster(name="baz", image=docker_image) as cluster2:
            with Client(cluster2) as client2:
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters_simultaneously(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="fizz", image=docker_image) as cluster1, KubeCluster(
            name="buzz", image=docker_image
        ) as cluster2:
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_multiple_clusters_simultaneously_same_loop(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="fizz", image=docker_image) as cluster1, KubeCluster(
            name="buzz", image=docker_image, loop=cluster1.loop
        ) as cluster2:
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert cluster1.loop is cluster2.loop is client1.loop is client2.loop
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_cluster_from_name(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="abc", image=docker_image) as firstcluster:
            with KubeCluster.from_name("abc") as secondcluster:
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
