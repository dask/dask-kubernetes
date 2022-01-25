.. _helmcluster:

HelmCluster
===========

:doc:`helmcluster` is for managing an existing Dask cluster which has been deployed using
`Helm <https://helm.sh>`_.

Quickstart
----------

.. currentmodule:: dask_kubernetes

First you must install the `Dask Helm chart <https://helm.dask.org/>`_ with ``helm``
and have the cluster running.

.. code-block:: bash

   helm repo add dask https://helm.dask.org
   helm repo update

   helm install myrelease dask/dask

You can then create a :class:`HelmCluster` object in Python to manage scaling the cluster and retrieve logs.

.. code-block:: python

   from dask_kubernetes import HelmCluster

   cluster = HelmCluster(release_name="myrelease")
   cluster.scale(10)  # specify number of workers explicitly

With this cluster object you can conveniently connect a Dask :class:`dask.distributed.Client` object to the cluster
and perform your work. Provided you have API access to Kubernetes and can run the ``kubectl`` command then
connectivity to the Dask cluster is handled automatically for you via services or port forwarding.

.. code-block:: python

    # Example usage
    from dask.distributed import Client
    import dask.array as da

    # Connect Dask to the cluster
    client = Client(cluster)

    # Create a large array and calculate the mean
    array = da.ones((1000, 1000, 1000))
    print(array.mean().compute())  # Should print 1.0

For more information see the :class:`HelmCluster` API reference.

.. warning::
    It is not possible to use ``HelmCluster`` from the Jupyter session
    which is deployed as part of the Helm Chart without first copying your
    ``~/.kube/config`` file to that Jupyter session.

API
---

.. currentmodule:: dask_kubernetes

.. autosummary::
   HelmCluster
   HelmCluster.scale
   HelmCluster.adapt
   HelmCluster.logs

.. autoclass:: HelmCluster
   :members:
