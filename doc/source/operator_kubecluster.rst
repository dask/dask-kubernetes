KubeCluster
===========

.. currentmodule:: dask_kubernetes.operator.kubecluster

.. note::

   As of ``2022.10.0`` the default ``KubeCluster`` class requires the :doc:`Dask Kubernetes Operator <operator>`. For documentation on the classic KubeCluster implementation :doc:`see here <kubecluster>`.

Cluster manager
---------------

The operator has a new cluster manager called :class:`dask_kubernetes.operator.kubecluster.KubeCluster` that you can use to conveniently create and manage a Dask cluster in Python. Then connect a Dask :class:`distributed.Client` object to it directly and perform your work.

The goal of the cluster manager is to abstract away the complexity of the Kubernetes resources and provide a clean and simple Python API to manager clusters while still getting all the benefits of the operator.

Under the hood the Python cluster manager will interact with ther Kubernetes API to create :doc:`custom resources <operator_resources>` for us.

To create a cluster in the default namespace, run the following

.. code-block:: python

   from dask_kubernetes import KubeCluster

   cluster = KubeCluster(name='foo')

You can change the default configuration of the cluster by passing additional args
to the python class (``namespace``, ``n_workers``, etc.) of your cluster. See the API reference :ref:`api`

You can scale the cluster

.. code-block:: python

   # Scale up the cluster
   cluster.scale(5)

   # Scale down the cluster
   cluster.scale(1)


You can autoscale the cluster

.. code-block:: python

   # Allow cluster to autoscale between 1 and 10 workers
   cluster.adapt(minimum=1, maximum=10)

   # Disable autoscaling by explicitly scaling to your desired number of workers
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

   from dask_kubernetes import KubeCluster

   cluster = KubeCluster(name='foo')

   cluster.add_worker_group(name="highmem", n_workers=2, resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}})

We can also scale the worker groups by name from the cluster object.

.. code-block:: python

   cluster.scale(5, worker_group="highmem")

Additional worker groups can also be deleted in Python.

.. code-block:: python

   cluster.delete_worker_group(name="highmem")

Any additional worker groups you create will be deleted when the cluster is deleted.

Custom cluster spec
-------------------

The ``KubeCluster`` class can take a selection of keyword arguments to make it quick and easy to get started, however the underlying :doc:`DaskCluster <operator_resources>` resource can be much more complex and configured in many ways.
Rather than exposing every possibility via keyword arguments instead you can pass a valid ``DaskCluster`` resource spec which will be used when creating the cluster.
You can also generate a spec with :func:`make_cluster_spec` which ``KubeCluster`` uses internally and then modify it with your custom options.


.. code-block:: python

   from dask_kubernetes import KubeCluster, make_cluster_spec

   config = {
      "name": "foo",
      "n_workers": 2,
      "resources":{"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}}
   }

   cluster = KubeCluster(**config)
   # is equivalent to
   cluster = KubeCluster(custom_cluster_spec=make_cluster_spec(**config))

You can also modify the spec before passing it to ``KubeCluster``, for example if you want to set ``nodeSelector`` on your worker pods you could do it like this:

.. code-block:: python

   from dask_kubernetes import KubeCluster, make_cluster_spec

   spec = make_cluster_spec(name="selector-example", n_workers=2)
   spec["spec"]["worker"]["spec"]["nodeSelector"] = {"disktype": "ssd"}

   cluster = KubeCluster(custom_cluster_spec=spec)

The ``cluster.add_worker_group()`` method also supports passing a ``custom_spec`` keyword argument which can be generated with :func:`make_worker_spec`.

.. code-block:: python

   from dask_kubernetes import KubeCluster, make_worker_spec

   cluster = KubeCluster(name="example")

   worker_spec = make_worker_spec(cluster_name=cluster.name, n_workers=2, resources={"limits": {"nvidia.com/gpu": 1}})
   worker_spec["spec"]["nodeSelector"] = {"cloud.google.com/gke-nodepool": "gpu-node-pool"}

   cluster.add_worker_group(custom_spec=worker_spec)


.. _api:

API
---

.. currentmodule:: dask_kubernetes.operator.kubecluster

.. autosummary::
   KubeCluster
   KubeCluster.scale
   KubeCluster.adapt
   KubeCluster.get_logs
   KubeCluster.add_worker_group
   KubeCluster.delete_worker_group
   KubeCluster.close

.. autoclass:: KubeCluster
   :members:

.. autofunction:: make_cluster_spec
.. autofunction:: make_worker_spec
