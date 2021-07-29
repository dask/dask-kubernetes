Testing
=======

Running the test suite for ``dask-kubernetes`` doesn't require an existing Kubernetes cluster but does require
`Docker <https://docs.docker.com/get-docker/>`_, `kubectl <https://kubernetes.io/docs/tasks/tools/#kubectl>`_ and `helm <https://helm.sh/docs/intro/install/>`_.

Start by installing dask-kubernetes in editable mode - this will ensure that pytest can import dask-kubernetes:

    $ pip install -e .


You will also need to install the test dependencies::

    $ pip install -r requirements-test.txt

Tests are run using `pytest <https://docs.pytest.org/en/stable/>`_::

    $ pytest
    ============================================== test session starts ==============================================
    platform darwin -- Python 3.8.8, pytest-6.2.2, py-1.10.0, pluggy-0.13.1 --
    cachedir: .pytest_cache
    rootdir: /Users/jtomlinson/Projects/dask/dask-kubernetes, configfile: setup.cfg
    plugins: anyio-2.2.0, asyncio-0.14.0, kind-21.1.3
    collected 64 items

    ...
    ================= 56 passed, 1 skipped, 6 xfailed, 1 xpassed, 53 warnings in 404.19s (0:06:44) ==================

Kind
----

To test ``dask-kubernetes`` against a real Kubernetes cluster we use the `pytest-kind <https://pypi.org/project/pytest-kind/>`_ plugin.

`Kind <https://kind.sigs.k8s.io/>`_ stands for Kubernetes in Docker and will create a full Kubernetes cluster within a single Docker container on your system.
Kubernetes will then make use of the lower level `containerd <https://containerd.io/>`_ runtime to start additional containers, so your Kubernetes pods will not
appear in your ``docker ps`` output.

By default we set the ``--keep-cluster`` flag in ``setup.cfg`` which means the Kubernetes container will persist between ``pytest`` runs
to avoid creation/teardown time. Therefore you may want to manually remove the container when you are done working on ``dask-kubernetes``::

    $ docker stop pytest-kind-control-plane
    $ docker rm pytest-kind-control-plane

When you run the tests for the first time a config file will be created called ``.pytest-kind/pytest-kind/kubeconfig`` which is used for authenticating
with the Kubernetes cluster running in the container. If you wish to inspect the cluster yourself for debugging purposes you can set the environment
variable ``KUBECONFIG`` to point to that file, then use ``kubectl`` or ``helm`` as normal::

    $ export KUBECONFIG=.pytest-kind/pytest-kind/kubeconfig
    $ kubectl get nodes
    NAME                        STATUS   ROLES                  AGE   VERSION
    pytest-kind-control-plane   Ready    control-plane,master   10m   v1.20.2
    $ helm list
    NAME    NAMESPACE       REVISION        UPDATED STATUS  CHART   APP VERSION

Docker image
------------

Within the test suite there is a fixture which creates a Docker image called ``dask-kubernetes:dev`` from `this Dockerfile <https://github.com/dask/dask-kubernetes/blob/main/ci/Dockerfile>`_.
This image will be imported into the kind cluster and then be used in all Dask clusters created.
This is the official Dask Docker image but with the very latest trunks of ``dask`` and ``distrubuted`` installed. It is recommended that you also have the
latest development install of those projects in your local development environment too.

This image may go stale over time so you might want to periodically delete it to ensure it gets recreated with the latest code changes::

   $ docker rmi dask-kubernetes:dev

Linting and formatting
----------------------

To accept Pull Requests to ``dask-kubernetes`` we require that they pass ``black`` formatting and ``flake8`` linting.

To save developer time we support using `pre-commit <https://pre-commit.com/>`_ which runs ``black`` and ``flake8`` every time
you attempt to locally commit code::

   $ pip install pre-commit
   $ pre-commit install
