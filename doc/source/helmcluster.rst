.. _helmcluster:

Helm Cluster
============

Quickstart
----------

.. currentmodule:: dask_kubernetes

.. code-block:: bash

   helm repo add dask https://helm.dask.org
   helm repo update

   helm install dask/dask --name myrelease

.. code-block:: python

   from dask_kubernetes import HelmCluster

   cluster = HelmCluster(release_name="myrelease")
   cluster.scale(10)  # specify number of workers explicitly

For more information see the :class:`HelmCluster` API reference.

.. warning::
    It is not possible to use ``HelmCluster`` from the Jupyter session
    which is deployed as part of the Helm Chart without first copying your
    ``~/.kube/config`` file to that Jupyter session.