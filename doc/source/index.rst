Dask Kubernetes
===============

.. currentmodule:: dask_kubernetes

Dask Kubernetes provides cluster managers for Kubernetes.

:class:`KubeCluster` deploys Dask clusters on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch ad-hoc deployments.

:class:`HelmCluster` is for managing an existing Dask cluster which has been deployed using
`Helm <https://helm.sh>`_. You must have already installed the `Dask Helm chart <https://helm.dask.org/>`_
and have the cluster running. You can then use it to manage scaling and retrieve logs.

For more general information on running Dask on Kubernetes see `this page <https://docs.dask.org/en/latest/setup/kubernetes.html>`_.

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
