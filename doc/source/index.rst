Dask Kubernetes
===============

.. image:: https://img.shields.io/pypi/v/dask-kubernetes
   :target: https://pypi.org/project/dask-kubernetes/
   :alt: PyPI

.. image:: https://img.shields.io/conda/vn/conda-forge/dask-kubernetes
   :target: https://anaconda.org/conda-forge/dask-kubernetes
   :alt: Conda Forge

.. image:: https://img.shields.io/badge/python%20support-3.8%7C3.9%7C3.10-blue
   :target: https://kubernetes.dask.org/en/latest/installing.html#supported-versions
   :alt: Python Support

.. image:: https://img.shields.io/badge/Kubernetes%20support-1.22%7C1.23%7C1.24%7C1.25-blue
   :target: https://kubernetes.dask.org/en/latest/installing.html#supported-versions
   :alt: Kubernetes Support


.. currentmodule:: dask_kubernetes

Welcome to the documentation for ``dask-kubernetes``.

.. note::

   If you are looking for general documentation on deploying
   Dask on Kubernetes new users should head to the
   `Dask documentation page on Kubernetes <https://docs.dask.org/en/latest/deploying-kubernetes.html>`_.

The package ``dask-kubernetes`` provides cluster managers for Kubernetes.

KubeCluster
-----------

:class:`KubeCluster` deploys Dask clusters on Kubernetes clusters using custom
Kubernetes resources.  It is designed to dynamically launch ad-hoc deployments.

.. code-block:: console

    $ # Install operator CRDs and controller, needs to be done once on your Kubernetes cluster
    $ helm repo add dask https://helm.dask.org && helm repo update
    $ kubectl create ns dask-operator
    $ helm install --namespace dask-operator dask-operator dask/dask-kubernetes-operator

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster
    cluster = KubeCluster(name="my_dask_cluster", image='ghcr.io/dask/dask:latest')
    cluster.scale(10)

HelmCluster
-----------

:class:`HelmCluster` is for managing an existing Dask cluster which has been deployed using
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

   operator_kubecluster
   helmcluster

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Operator

   operator
   operator_installation
   operator_resources
   operator_extending

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Classic

   kubecluster
   kubecluster_migrating

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Developer

   testing
   releasing
   history
