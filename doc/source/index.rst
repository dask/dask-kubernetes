Daskernetes
===========

Daskernetes deploys Dask workers on Kubernetes clusters

Install
-------

::

   pip install daskernetes

Quickstart
----------

.. code-block:: yaml

      # worker-spec.yml

      kind: Pod
      metadata:
        labels:
          foo: bar
      spec:
        restart_policy: Never
        containers:
        - image: daskdev/dask:latest
          args: [dask-worker, --nthreads, '2', --no-bokeh, --memory-limit, 8GB, --death-timeout, '60']
          env:
            - name: EXTRA_PIP_PACKAGES
              value: fastparquet git+https://github.com/dask/distributed
          resources:
            limits:
              cpu: "3"
              memory: 8G
            requests:
              cpu: "2"
              memory: 8G

.. code-block:: python

   from daskernetes import KubeCluster

   cluster = KubeCluster.from_yaml('worker-spec.yml')
   cluster.scale_up(10)  # specify number of nodes explicitly

   cluster.adapt()  # or dynamically scale based on current workload


Considerations
--------------

1.  Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See :obj:`KubeCluster` docstring below for guidance on how to check and
    modify this.

2.  Your Kubernetes resource limits and requests should match the
    ``--memory-limit`` and ``--nthreads`` parameters given to the
    ``dask-worker`` command.  Otherwise your workers may get killed by
    Kubernetes as they pack into the same node and overwhelm that nodes'
    available memory, leading to ``KilledWorker`` errors.


Configuration
-------------

There are a few special environment variables that affect daskernetes behavior:

1.  ``DASKERNETES_WORKER_TEMPLATE_PATH``: a path a a YAML file that holds a
    Pod spec for the worker.  If provided then this will be used when
    :obj:`KubeCluster` is called with no arguments::

       cluster = KubeCluster()  # reads provided yaml file

2.  ``DASKERNETES_DIAGNOSTICS_LINK``: a Python pre-formatted string that shows
    the location of Dask's dashboard.  This string will receive values for
    ``host``, ``port``, and all environment variables.  This is useful when
    using Daskernetes with JupyterHub and nbserverproxy to route the dashboard
    link to a proxied address as follows::

       export DASKERNETES_DIANGOSTICS_LINK="{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status"

Any other environment variable starting with ``DASKERNETES_`` will be placed in
the ``daskernetes.config`` dictionary for general use.


API Documentation
-----------------

.. currentmodule:: daskernetes

.. autosummary::
   KubeCluster
   KubeCluster.adapt
   KubeCluster.from_dict
   KubeCluster.from_yaml
   KubeCluster.logs
   KubeCluster.pods
   KubeCluster.scale

.. autoclass:: KubeCluster
   :members:
