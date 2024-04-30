Dask Kubernetes Operator
========================

.. image:: https://img.shields.io/pypi/v/dask-kubernetes
   :target: https://pypi.org/project/dask-kubernetes/
   :alt: PyPI

.. image:: https://img.shields.io/conda/vn/conda-forge/dask-kubernetes
   :target: https://anaconda.org/conda-forge/dask-kubernetes
   :alt: Conda Forge

.. image:: https://img.shields.io/badge/python%20support-3.9%7C3.10%7C3.11%7C3.12-blue
   :target: https://kubernetes.dask.org/en/latest/installing.html#supported-versions
   :alt: Python Support

.. image:: https://img.shields.io/badge/Kubernetes%20support-1.26%7C1.27%7C1.28%7C1.29-blue
   :target: https://kubernetes.dask.org/en/latest/installing.html#supported-versions
   :alt: Kubernetes Support


.. currentmodule:: dask_kubernetes

Welcome to the documentation for the Dask Kubernetes Operator.

.. note::

   If you are looking for high-level documentation on deploying
   Dask on Kubernetes new users should head to the
   `Dask documentation page on Kubernetes <https://docs.dask.org/en/latest/deploying-kubernetes.html>`_.

The package ``dask-kubernetes`` provides a Dask operator for Kubernetes. ``dask-kubernetes`` is one of many options for deploying Dask clusters, see `Deploying Dask <https://docs.dask.org/en/stable/deploying.html#distributed-computing>`_ in the Dask documentation for an overview of additional options.

KubeCluster
-----------

:class:`KubeCluster` deploys Dask clusters on Kubernetes clusters using custom
Kubernetes resources.  It is designed to dynamically launch ad-hoc deployments.

.. code-block:: console

    $ # Install operator CRDs and controller, needs to be done once on your Kubernetes cluster
    $ helm install --repo https://helm.dask.org --create-namespace -n dask-operator --generate-name dask-kubernetes-operator

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster
    cluster = KubeCluster(name="my-dask-cluster", image='ghcr.io/dask/dask:latest')
    cluster.scale(10)

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Installing

   installing
   operator_installation

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Operator

   operator
   operator_kubecluster
   operator_resources
   operator_extending
   operator_troubleshooting

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Classic

   kubecluster_migrating

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Developer

   testing
   releasing
   history
