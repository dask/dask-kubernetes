Dask Kubernetes
===============

Dask Kubernetes deploys Dask workers on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch short-lived deployments
of workers during the lifetime of a Python process.

Install
-------

::

   pip install dask-kubernetes

Quickstart
----------

.. code-block:: python

   from dask_kubernetes import KubeCluster

   cluster = KubeCluster.from_yaml('worker-spec.yml')
   cluster.scale_up(10)  # specify number of nodes explicitly

   cluster.adapt()  # or dynamically scale based on current workload

.. code-block:: yaml

      # worker-spec.yml

      kind: Pod
      metadata:
        labels:
          foo: bar
      spec:
        restartPolicy: Never
        containers:
        - image: daskdev/dask:latest
          args: [dask-worker, --nthreads, '2', --no-bokeh, --memory-limit, 6GB, --death-timeout, '60']
          env:
            - name: EXTRA_PIP_PACKAGES
              value: fastparquet git+https://github.com/dask/distributed
          resources:
            limits:
              cpu: "2"
              memory: 6G
            requests:
              cpu: "2"
              memory: 6G


Best Practices
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

3.  We recommend adding the ``--death-timeout, '60'`` arguments and the
    ``restartPolicy: Never`` attribute to your worker specification.
    This ensures that these pods will clean themselves up if your Python
    process disappears unexpectedly.


Configuration
-------------

There are a few special environment variables that affect dask-kubernetes behavior:

1.  ``DASK_KUBERNETES_WORKER_TEMPLATE_PATH``: a path to a YAML file that holds a
    Pod spec for the worker.  If provided then this will be used when
    :obj:`KubeCluster` is called with no arguments::

       cluster = KubeCluster()  # reads provided yaml file

2.  ``DASK_DIAGNOSTICS_LINK``: a Python pre-formatted string that shows
    the location of Dask's dashboard.  This string will receive values for
    ``host``, ``port``, and all environment variables.  This is useful when
    using dask-kubernetes with JupyterHub and nbserverproxy to route the dashboard
    link to a proxied address as follows::

       export DASK_DIANGOSTICS_LINK="{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status"

    This is inherited from general Dask behavior.

3.  ``DASK_KUBERNETES_WORKER_NAME``: a Python pre-formatted string to use
    when naming dask worker pods. This string will receive values for ``user``,
    ``uuid``, and all environment variables. This is useful when you want to have
    control over the naming convention for your pods and use other tokens from
    the environment. For example when using zero-to-jupyterhub every user is
    called ``jovyan`` and so you may wish to use ``dask-{JUPYTERHUB_USER}-{uuid}``
    instead of ``dask-{user}-{uuid}``. **Ensure you keep the ``uuid`` somewhere in
    the template.**

Any other environment variable starting with ``DASK_`` will be placed in
the ``dask.distributed.config`` dictionary for general use.


Docker Images
-------------

Example Dask docker images daskdev/dask and daskdev/dask-notebook
are available on https://hub.docker.com/r/daskdev .
More information about these images is available at the
`Dask documentation <http://dask.pydata.org/en/latest/setup/docker.html>`_.


.. toctree::
   :maxdepth: 1
   :hidden:

   api
   history
   testing
