from dask.distributed import Client

from dask_kubernetes.experimental import KubeCluster


def test_kubecluster(cluster):
    with Client(cluster) as client:
        client.scheduler_info()
        cluster.scale(1)
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
        with KubeCluster(name="bar", image=docker_image) as cluster1, KubeCluster(
            name="baz", image=docker_image
        ) as cluster2:
            with Client(cluster1) as client1, Client(cluster2) as client2:
                assert client1.submit(lambda x: x + 1, 10).result() == 11
                assert client2.submit(lambda x: x + 1, 10).result() == 11


def test_cluster_from_name(kopf_runner, docker_image):
    with kopf_runner:
        with KubeCluster(name="bar", image=docker_image) as firstcluster:
            with KubeCluster.from_name("bar") as secondcluster:
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
