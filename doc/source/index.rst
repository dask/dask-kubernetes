Dask Kubernetes
===============

.. currentmodule:: dask_kubernetes

Welcome to the documentation for ``dask-kubernetes``.

.. note::

   If you are looking for general documentation on deploying
   Dask on Kubernetes new users should head to the
   `Dask documentation page on Kubernetes <https://docs.dask.org/en/latest/deploying-kubernetes.html>`_.

The package ``dask-kubernetes`` provides cluster managers for Kubernetes.

:doc:`kubecluster` deploys Dask clusters on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch ad-hoc deployments.

.. code-block:: python

    from dask_kubernetes import KubeCluster, make_pod_spec

    pod_spec = make_pod_spec(image='daskdev/dask:latest')
    cluster = KubeCluster(pod_spec)
    cluster.scale(10)

:doc:`helmcluster` is for managing an existing Dask cluster which has been deployed using
`Helm <https://helm.sh>`_. You must have already installed the `Dask Helm chart <https://helm.dask.org/>`_
and have the cluster running. You can then use it to manage scaling and retrieve logs.

.. code-block:: python

   from dask_kubernetes import HelmCluster

   cluster = HelmCluster(release_name="myrelease")
   cluster.scale(10)

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Overview

   installing

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Cluster Managers

   kubecluster
   helmcluster

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Developer

   testing
   releasing
   history
