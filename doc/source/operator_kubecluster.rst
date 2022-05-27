KubeCluster (experimental)
==========================
.. currentmodule:: dask_kubernetes.experimental

.. warning::
    The Dask Operator for Kubernetes is experimental. So any `bug reports <https://github.com/dask/dask-kubernetes/issues>`_ are appreciated!

Cluster manager
---------------

The operator has a new cluster manager called :class:`dask_kubernetes.experimental.KubeCluster` that you can use to conveniently create and manage a Dask cluster in Python. Then connect a :class:`dask.distributed.Client` object to it directly and perform your work.

The goal of the cluster manager is to abstract away the complexity of the Kubernetes resources and provide a clean and simple Python API to manager clusters while still getting all the benefits of the operator.

Under the hood the Python cluster manager will interact with ther Kubernetes API to create :doc:`custom resources <operator_resources>` for us.

To create a cluster in the default namespace, run the following

.. code-block:: python

   from dask_kubernetes.experimental import KubeCluster

   cluster = KubeCluster(name='foo')

You can change the default configuration of the cluster by passing additional args
to the python class (``namespace``, ``n_workers``, etc.) of your cluster. See the API reference :ref:`api`

You can scale the cluster

.. code-block:: python

   # Scale up the cluster
   cluster.scale(5)

   # Scale down the cluster
   cluster.scale(1)

You can connect to the client

.. code-block:: python

    from dask.distributed import Client

    # Connect Dask to the cluster
    client = Client(cluster)

Finally delete the cluster by running

.. code-block:: python

   cluster.close()

Additional worker groups
------------------------

Additional worker groups can also be created via the cluster manager in Python.

.. code-block:: python

   from dask_kubernetes.experimental import KubeCluster

   cluster = KubeCluster(name='foo')

   cluster.add_worker_group(name="highmem", n_workers=2, resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}})

We can also scale the worker groups by name from the cluster object.

.. code-block:: python

   cluster.scale(5, worker_group="highmem")

Additional worker groups can also be deleted in Python.

.. code-block:: python

   cluster.delete_worker_group(name="highmem")

Any additional worker groups you create will be deleted when the cluster is deleted.



.. _api:

API
---

.. currentmodule:: dask_kubernetes.experimental

.. autosummary::
   KubeCluster
   KubeCluster.scale
   KubeCluster.get_logs
   KubeCluster.add_worker_group
   KubeCluster.delete_worker_group
   KubeCluster.close

.. autoclass:: KubeCluster
   :members:
