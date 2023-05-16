
Contributing to Dask Kubernetes
===============================
Dask Kubernetes is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more. This page provides resources on how best to contribute.

Development environment
-----------------------

Download code
~~~~~~~~~~~~~

Make a fork of the main `Dask Kubernetes repository <https://github.com/Matt711/dask-kubernetes>`_ and
clone the fork::

   git clone git@github.com:<your-github-username>/dask-kubernetes.git
   cd dask-kubernetes

You should also add the remote upstream repository::

   git remote add upstream git@github.com:dask/dask-kubernetes.git

Contributions to Dask Kubernetes can then be made by submitting pull requests on GitHub.

Testing
~~~~~~~

Start by installing dask-kubernetes in editable mode - this will ensure that pytest can import dask-kubernetes:::

   $ pip install -e .

Install the required packages at the top level of your cloned repository::
    
   $ pip install -r requirements.txt

Running the test suite for dask-kubernetes doesn’t require an existing Kubernetes cluster but does require [Docker](https://docs.docker.com/get-docker/), [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) and [helm](https://helm.sh/docs/intro/install/). Tests are run using `pytest <https://docs.pytest.org/en/stable/>`_::

    $ pytest
    ============================================== test session starts ==============================================
    platform darwin -- Python 3.8.8, pytest-6.2.2, py-1.10.0, pluggy-0.13.1 --
    cachedir: .pytest_cache
    rootdir: /Users/jtomlinson/Projects/dask/dask-kubernetes, configfile: setup.cfg
    plugins: anyio-2.2.0, asyncio-0.14.0, kind-21.1.3
    collected 64 items

    ...
    ================= 56 passed, 1 skipped, 6 xfailed, 1 xpassed, 53 warnings in 404.19s (0:06:44) ==================

Testing the operator
''''''''''''''''''''

You can test the Dask Operator with `pytest`::

   $ pytest dask_kubernetes/operator/
   =============================================== test session starts ==============================================
   platform linux -- Python 3.10.9, pytest-7.1.2, pluggy-1.0.0 -- /home/mmurray/anaconda3/bin/python
   cachedir: .pytest_cache
   rootdir: /home/mmurray/dask-kubernetes, configfile: setup.cfg
   plugins: anyio-3.5.0, rerunfailures-11.1.2, asyncio-0.21.0, kind-22.11.1, timeout-2.1.0
   asyncio: mode=strict
   timeout: 300.0s
   timeout method: signal
   timeout func_only: False
   collected 24 items

   ...

You can also start the controller in one terminal window with `kopf`::

   $ python -m kopf run dask_kubernetes/operator/controller/controller.py --verbose
   [2023-05-15 12:50:51,156] kopf._core.reactor.r [DEBUG   ] Starting Kopf 1.36.1.
   [2023-05-15 12:50:51,156] kopf.activities.star [DEBUG   ] Activity 'noop_startup' is invoked.
   [2023-05-15 12:50:51,156] kopf.activities.star [INFO    ] Plugin 'noop' running. This does nothing. See https://kubernetes.dask.org/en/latest/operator_extending.html for details on writing plugins for the Dask Operator controller.
   [2023-05-15 12:50:51,156] kopf.activities.star [INFO    ] Activity 'noop_startup' succeeded.
   [2023-05-15 12:50:51,156] kopf.activities.star [DEBUG   ] Activity 'startup' is invoked.

   ...

And create a KubeCluster in another window::

   >>> from dask_kubernetes.operator import KubeCluster
   >>> cluster = KubeCluster(name="foo")
   

Stability
'''''''''

Continuous integration testing
''''''''''''''''''''''''''''''

Documentation
'''''''''''''