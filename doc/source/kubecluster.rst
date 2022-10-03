.. _kubecluster:

KubeCluster (classic)
=====================

.. Warning::

   This implementation of ``KubeCluster`` is being retired and we recommend :doc:`migrating to the operator based implementation <kubecluster_migrating>`.


:class:`KubeCluster` deploys Dask clusters on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch ad-hoc deployments.

Quickstart
----------

.. currentmodule:: dask_kubernetes

To launch a Dask cluster on Kubernetes with :class:`KubeCluster` you need to first configure your worker
pod specification. Then create a cluster with that spec.

.. code-block:: python

    from dask_kubernetes.classic import KubeCluster, make_pod_spec

    pod_spec = make_pod_spec(image='ghcr.io/dask/dask:latest',
                             memory_limit='4G', memory_request='4G',
                             cpu_limit=1, cpu_request=1)

    cluster = KubeCluster(pod_spec)

    cluster.scale(10)  # specify number of workers explicitly
    cluster.adapt(minimum=1, maximum=100)  # or dynamically scale based on current workload

You can then connect a Dask :class:`dask.distributed.Client` object to the cluster and perform your work.

.. code-block:: python

    # Example usage
    from dask.distributed import Client
    import dask.array as da

    # Connect Dask to the cluster
    client = Client(cluster)

    # Create a large array and calculate the mean
    array = da.ones((1000, 1000, 1000))
    print(array.mean().compute())  # Should print 1.0

You can alternatively define your worker specification via YAML by creating a `pod manifest <https://kubernetes.io/docs/concepts/workloads/pods/>`_
that will be used as a template.

.. code-block:: yaml

    # worker-spec.yml

    kind: Pod
    metadata:
      labels:
        foo: bar
    spec:
      restartPolicy: Never
      containers:
      - image: ghcr.io/dask/dask:latest
        imagePullPolicy: IfNotPresent
        args: [dask-worker, --nthreads, '2', --no-dashboard, --memory-limit, 6GB, --death-timeout, '60']
        name: dask-worker
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

   from dask_kubernetes.classic import KubeCluster

   cluster = KubeCluster('worker-spec.yml')
   cluster.scale(10)

For more information see the :class:`KubeCluster` API reference.

Best Practices
--------------

1.  Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See :py:class:`dask_kubernetes.operator.kubecluster` docstring for guidance on how
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
            nvidia.com/gpu: 1 # requesting 1 GPU

.. _configuration:
Configuration
-------------

You can use `Dask's configuration <https://docs.dask.org/en/latest/configuration.html>`_
to control the behavior of Dask-kubernetes.  You can see a full set of
configuration options
`here <https://github.com/dask/dask-kubernetes/blob/main/dask_kubernetes/kubernetes.yaml>`_.
Some notable ones are described below:

1.  ``kubernetes.worker-template-path``: a path to a YAML file that holds a
    Pod spec for the worker.  If provided then this will be used when
    :py:class:`dask_kubernetes.operator.kubecluster` is called with no arguments::

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

In order to spawn a Dask cluster, the service account creating those pods will require
a set of RBAC permissions. Create a service account you will use for Dask, and then attach the
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
    - apiGroups:
      - "policy"  # indicates the policy API group
      resources:
      - "poddisruptionbudgets"
      verbs:
      - "get"
      - "list"
      - "watch"
      - "create"
      - "delete"


Docker Images
-------------

Example Dask docker images ghcr.io/dask/dask and ghcr.io/dask/dask-notebook
are available on https://github.com/orgs/dask/packages .
More information about these images is available at the
`Dask documentation <https://docs.dask.org/en/latest/setup/docker.html>`_.

Note that these images can be further customized with extra packages using
``EXTRA_PIP_PACKAGES``, ``EXTRA_APT_PACKAGES``, and ``EXTRA_CONDA_PACKAGES``
as described in the
`Extensibility section <https://docs.dask.org/en/latest/setup/docker.html#extensibility>`_.

Deployment Details
------------------

Scheduler
~~~~~~~~~

Before workers are created a scheduler will be deployed with the following resources:

- A pod with a scheduler running
- A service (svc) to expose scheduler and dashboard ports
- A PodDisruptionBudget avoid voluntary disruptions of the scheduler pod

By default the Dask configuration option ``kubernetes.scheduler-service-type`` is
set to ``ClusterIp``. In order to connect to the scheduler the ``KubeCluster`` will first attempt to connect directly,
but this will only be successful if ``dask-kubernetes`` is being run from within the Kubernetes cluster.
If it is unsuccessful it will attempt to port forward the service locally using the ``kubectl`` utility.

If you update the service type to ``NodePort``. The scheduler will be exposed on the same random high port on all
nodes in the cluster. In this case ``KubeCluster`` will attempt to list nodes in order to get an IP to connect on
and requires additional permissions to do so.

.. code-block:: yaml

    - apiGroups:
      - ""  # indicates the core API group
      resources:
      - "nodes"
      verbs:
      - "get"
      - "list"


If you set the service type to ``LoadBalancer`` then ``KubeCluster`` will connect to the external address of the assigned
loadbalancer, but this does require that your Kubernetes cluster has the appropriate operator to assign loadbalancers.

Legacy mode
^^^^^^^^^^^

For backward compatibility with previous versions of ``dask-kubernetes`` it is also possible to run the scheduler locally.
A ``local`` scheduler is created where the Dask client will be created.

.. code-block:: python

   from dask_kubernetes.classic import KubeCluster
   from dask.distributed import Client

   cluster = KubeCluster.from_yaml('worker-spec.yml', deploy_mode='local')
   cluster.scale(10)
   client = Client(cluster)

In this mode the Dask workers will attempt to connect to the machine where you are running ``dask-kubernetes``.
Generally this will need to be within the Kubernetes cluster in order for the workers to make a successful connection.

Workers
~~~~~~~

Workers are created directly as simple pods.  These worker pods are configured
to shutdown if they are unable to connect to the scheduler for 60 seconds.
The pods are cleaned up when :meth:`~dask_kubernetes.operator.kubecluster.close` is called,
or the scheduler process exits.

The pods are created with two default `tolerations <https://kubernetes.io/docs/concepts/configuration/taint-and-toleration/>`_:

* ``k8s.dask.org/dedicated=worker:NoSchedule``
* ``k8s.dask.org_dedicated=worker:NoSchedule``

If you have nodes with the corresponding taints, then the worker pods will
schedule to those nodes (and no other pods will be able to schedule to those
nodes).

API
---

.. currentmodule:: dask_kubernetes

.. autosummary::
   KubeCluster
   KubeCluster.adapt
   KubeCluster.from_dict
   KubeCluster.from_yaml
   KubeCluster.get_logs
   KubeCluster.pods
   KubeCluster.scale
   InCluster
   KubeConfig
   KubeAuth
   make_pod_spec

.. autoclass:: KubeCluster
   :members:

.. autoclass:: ClusterAuth
   :members:

.. autoclass:: InCluster

.. autoclass:: KubeConfig

.. autoclass:: KubeAuth

.. autofunction:: make_pod_spec
