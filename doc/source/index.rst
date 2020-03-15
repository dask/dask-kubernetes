Dask Kubernetes
===============

Dask Kubernetes deploys Dask workers on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch short-lived deployments
of workers during the lifetime of a Python process.

Alternatively, you can deploy a Dask Cluster on Kubernetes using `Helm <https://helm.sh>`_.
See https://docs.dask.org/en/latest/setup/kubernetes.html for more.

Currently, it is designed to be run from a pod on a Kubernetes cluster that
has permissions to launch other pods. However, it can also work with a remote
Kubernetes cluster (configured via a kubeconfig file), as long as it is possible
to interact with the Kubernetes API and access services on the cluster.

Installing
----------

You can install dask-kubernetes with ``pip``, ``conda``, or by installing from source.

Pip
---

Pip can be used to install both dask-jobqueue and its dependencies.::

   pip install dask-kubernetes --upgrade # Install everything from last released version

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

You can also install directly from git master branch::

    pip install git+https://github.com/dask/dask-kubernetes

Quickstart
----------

.. code-block:: python

   from dask_kubernetes import KubeCluster

   cluster = KubeCluster.from_yaml('worker-spec.yml')
   cluster.scale(10)  # specify number of workers explicitly

   cluster.adapt(minimum=1, maximum=100)  # or dynamically scale based on current workload

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
          imagePullPolicy: IfNotPresent
          args: [dask-worker, --nthreads, '2', --no-dashboard, --memory-limit, 6GB, --death-timeout, '60']
          name: dask
          env:
            - name: EXTRA_PIP_PACKAGES
              value: git+https://github.com/dask/distributed
          resources:
            limits:
              cpu: "2"
              memory: 6G
            requests:
              cpu: "2"
              memory: 6G

.. code-block:: python

        # Example usage
        from dask.distributed import Client
        import dask.array as da

        # Connect Dask to the cluster
        client = Client(cluster)

        # Create a large array and calculate the mean
        array = da.ones((1000, 1000, 1000))
        print(array.mean().compute())  # Should print 1.0


Best Practices
--------------

1.  Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See :py:class:`dask_kubernetes.KubeCluster` docstring for guidance on how 
    to check and modify this.

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

You can use `Dask's configuration <https://docs.dask.org/en/latest/configuration.html>`_
to control the behavior of Dask-kubernetes.  You can see a full set of
configuration options
`here <https://github.com/dask/dask-kubernetes/blob/master/dask_kubernetes/kubernetes.yaml>`_.
Some notable ones are described below:

1.  ``kubernetes.worker-template-path``: a path to a YAML file that holds a
    Pod spec for the worker.  If provided then this will be used when
    :py:class:`dask_kubernetes.KubeCluster` is called with no arguments::

       cluster = KubeCluster()  # reads provided yaml file

2.  ``distributed.dashboard.link``: a Python pre-formatted string that shows
    the location of Dask's dashboard.  This string will receive values for
    ``host``, ``port``, and all environment variables.

    For example this is useful when using dask-kubernetes with JupyterHub and
    `nbserverproxy <https://github.com/jupyterhub/nbserverproxy>`_ to route the
    dashboard link to a proxied address as follows::

       "{JUPYTERHUB_SERVICE_PREFIX}proxy/{port}/status"

3.  ``kubernetes.worker-name``: a Python pre-formatted string to use
    when naming dask worker pods. This string will receive values for ``user``,
    ``uuid``, and all environment variables. This is useful when you want to have
    control over the naming convention for your pods and use other tokens from
    the environment. For example when using zero-to-jupyterhub every user is
    called ``jovyan`` and so you may wish to use ``dask-{JUPYTERHUB_USER}-{uuid}``
    instead of ``dask-{user}-{uuid}``. **Ensure you keep the ``uuid`` somewhere in
    the template.**

Role-Based Access Control (RBAC)
--------------------------------

In order to spawn worker (or separate scheduler) Dask pods, the service
account creating those pods will require a set of RBAC permissions.
Create a service account you will use for Dask, and then attach the
following Role to that ServiceAccount via a RoleBinding:

.. code-block:: yaml

    kind: Role
    apiVersion: rbac.authorization.k8s.io/v1beta1
    metadata:
      name: daskKubernetes
    rules:
    - apiGroups:
      - ""  # indicates the core API group
      resources:
      - "pods"
      verbs:
      - "get"
      - "list"
      - "watch"
      - "create"
      - "delete"
    - apiGroups:
      - ""  # indicates the core API group
      resources:
      - "pods/log"
      verbs:
      - "get"
      - "list"

If you intend to use the newer Dask functionality in which the scheduler
is created in its own pod and accessed via a service, you will also
need:

.. code-block:: yaml

    - apiGroups:
      - "" # indicates the core API group
      resources:
      - "services"
      verbs:
      - "get"
      - "list"
      - "watch"
      - "create"
      - "delete"


Docker Images
-------------

Example Dask docker images daskdev/dask and daskdev/dask-notebook
are available on https://hub.docker.com/r/daskdev .
More information about these images is available at the
`Dask documentation <https://docs.dask.org/en/latest/setup/docker.html>`_.

Note that these images can be further customized with extra packages using
``EXTRA_PIP_PACKAGES``, ``EXTRA_APT_PACKAGES``, and ``EXTRA_CONDA_PACKAGES``
as described in the
`Extensibility section <https://docs.dask.org/en/latest/setup/docker.html#extensibility>`_.

Deployment Details
------------------

Workers are created directly as simple pods.  These worker pods are configured
to shutdown if they are unable to connect to the scheduler for 60 seconds.
The pods are cleaned up when :meth:`~dask_kubernetes.KubeCluster.close` is called,
or the scheduler process exits.

The pods are created with two default `tolerations <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>`_:

* ``k8s.dask.org/dedicated=worker:NoSchedule``
* ``k8s.dask.org_dedicated=worker:NoSchedule``

If you have nodes with the corresponding taints, then the worker pods will
schedule to those nodes (and no other pods will be able to schedule to those
nodes).

.. toctree::
   :maxdepth: 1
   :hidden:

   api
   history
   testing
