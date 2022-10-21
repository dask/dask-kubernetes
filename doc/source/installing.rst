Installing
==========

.. currentmodule:: dask_kubernetes

You can install dask-kubernetes with ``pip``, ``conda``, or by installing from source.

Dependencies
------------

To use :class:`KubeCluster` you may need to have ``kubectl`` installed (`official install guide <https://kubernetes.io/docs/tasks/tools/#kubectl>`_).

To use :class:`HelmCluster` you will need to have ``helm`` installed (`official install guide <https://helm.sh/docs/intro/install/>`_).

Pip
---

Pip can be used to install dask-kubernetes and its Python dependencies::

   pip install dask-kubernetes --upgrade  # Install everything from last released version

Conda
-----

To install the latest version of dask-kubernetes from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.anaconda.com/downloads>`_::

    conda install dask-kubernetes -c conda-forge

Install from Source
-------------------

To install dask-kubernetes from source, clone the repository from `github
<https://github.com/dask/dask-kubernetes>`_::

    git clone https://github.com/dask/dask-kubernetes.git
    cd dask-kubernetes
    python setup.py install

or use ``pip`` locally if you want to install all dependencies as well::

    pip install -e .

You can also install directly from git main branch::

    pip install git+https://github.com/dask/dask-kubernetes

Supported Versions
------------------

Python
^^^^^^

All Dask projects generally follow the `NEP 29 <https://numpy.org/neps/nep-0029-deprecation_policy.html>`_ deprecation policy for Python where each Python minor version is support ed for 42 months.
Due to Python's 12 month release cycle this ensures at least the current version and two previous versions are supported.

The Dask Kubernetes CI tests all PRs against all supported Python versions.

Kubernetes
^^^^^^^^^^

For Kubernetes we follow the `yearly support KEP <https://kubernetes.io/releases/patch-releases/#support-period>`_.
Due to the 4-6 month release cycle this also ensures that at least the current and two previous versions are supported.

The Dask Kubernetes CI tests all PRs against all supported Kubernetes versions.

.. note::

    To keep the CI matrix smaller we test all Kubernetes versions against the latest Python, and all Python versions against the latest Kubernetes.
    We do not test older versions of Python and Kubernetes together. See `dask/dask-kubernetes#559 <https://github.com/dask/dask-kubernetes/pull/559>`_ for more information.
