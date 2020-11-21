.. _kubecluster:

Kube Cluster
============

Quickstart
----------

.. currentmodule:: dask_kubernetes

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

For more information see the :class:`KubeCluster` API reference.

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

GPUs
----

Because ``dask-kubernetes`` uses standard kubernetes pod specifications, we can
use `kubernetes device plugins
<https://kubernetes.io/docs/tasks/manage-gpus/scheduling-gpus/#using-device-plugins>`_
and add resource limits defining the number of GPUs per pod/worker.
Additionally, we can also use tools like `dask-cuda
<https://dask-cuda.readthedocs.io/>`_ for optimized Dask/GPU interactions.

.. code-block:: yaml

      kind: Pod
      metadata:
        labels:
          foo: bar
      spec:
        restartPolicy: Never
        containers:
        - image: rapidsai/rapidsai:cuda11.0-runtime-ubuntu18.04-py3.8
          imagePullPolicy: IfNotPresent
          args: [dask-cuda-worker, $(DASK_SCHEDULER_ADDRESS), --rmm-pool-size, 10GB]
          name: dask-cuda
          resources:
            limits:
              cpu: "2"
              memory: 6G
              nvidia.com/gpu: 1 # requesting 1 GPU
            requests:
              cpu: "2"
              memory: 6G

.. _configuration:
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

Workers
~~~~~~~

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

Scheduler
~~~~~~~~~

The scheduler can be deployed locally (default) or remotely.  A ``local``
scheduler is created where the Dask client will be created.


.. code-block:: python

   from dask_kubernetes import KubeCluster
   from dask.distributed import Client

   cluster = KubeCluster.from_yaml('worker-spec.yml', deploy_mode='local')
   cluster.scale(10)
   client = Client(cluster)

The scheduler can also be deployed on the kubernetes cluster with
``deploy_mode=remote``:


.. code-block:: python

   import dask
   from dask_kubernetes import KubeCluster
   from dask.distributed import Client

   cluster = KubeCluster.from_yaml('worker-spec.yml', deploy_mode='remote')
   cluster.scale(10)
   client = Client(cluster)


When deploying remotely, the following k8s resources are created:

- A pod with a scheduler running
- (optional) A pod with a LoadBalancer and complimentary service (svc) to
  expose scheduler and dashboard ports
- A PodDisruptionBudget avoid voluntary disruptions of the scheduler pod

By default, the configuration option, ``scheduler-service-type``, is
set to ``ClusterIp``. To optionally use a LoadBalancer, change ``scheduler-service-type`` to
``LoadBalancer``.  This change can either be done with the :ref:`dask-kubernetes
configuration file<configuration>` or programmatically with ``dask.config.set``:

.. code-block:: python

   dask.config.set({"kubernetes.scheduler-service-type": "LoadBalancer"})

