
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
from dask.distributed import Client
from dask_kubernetes.operator import KubeCluster
cluster = KubeCluster(name='cluster2', image='ghcr.io/patmagauran/mldocker-base:main',env={'EXTRA_CONDA_PACKAGES':'gcsfs'}  )
#cluster = KubeCluster.from_name(name="foo")
cluster.adapt(minimum=1, maximum=10)
#cluster.scale(10)
# Scale up: connect to your own cluster with more resources
# see http://dask.pydata.org/en/latest/setup.html
client = Client(cluster)
client