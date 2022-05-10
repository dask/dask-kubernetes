from dask.distributed import Client
from dask_ctl import get_cluster


def test_discovery(cluster):
    with get_cluster(cluster.name) as cluster2:
        assert cluster == cluster2
        assert "id" in cluster2.scheduler_info
        with Client(cluster2) as client:
            assert client.submit(lambda x: x + 1, 10).result() == 11
