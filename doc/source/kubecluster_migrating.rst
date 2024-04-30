Migrating from classic
======================

The classic ``KubeCluster`` class has been replaced with a new version that is built using the Kubernetes Operator pattern.


Installing the operator
-----------------------

To use the new implementation of ``KubeCluster`` you need to :doc:`install the Dask operator custom resources and controller <operator_installation>`.

The custom resources allow us to describe our Dask cluster components as native Kubernetes resources rather than directly creating ``Pod`` and ``Service`` resources like the classic implementation does.

Unfortunately this requires a small amount of first time setup on you Kubernetes cluster before you can start using ``dask-kubernetes``. This is a key reason why the new implementation has breaking changes.
The quickest way to install things is with ``helm``.

.. code-block:: console

    $ helm repo add dask https://helm.dask.org
    "dask" has been added to your repositories

    $ helm repo update
    Hang tight while we grab the latest from your chart repositories...
    ...Successfully got an update from the "dask" chart repository
    Update Complete. ⎈Happy Helming!⎈

    $ helm install --create-namespace -n dask-operator --generate-name dask/dask-kubernetes-operator
    NAME: dask-kubernetes-operator-1666875935
    NAMESPACE: dask-operator
    STATUS: deployed
    REVISION: 1
    TEST SUITE: None
    NOTES:
    Operator has been installed successfully.

Now that you have the controller and CRDs installed on your cluster you can start using the new :class:`dask_kubernetes.operator.KubeCluster`.

Using the new KubeCluster
-------------------------

The way you create clusters with ``KubeCluster`` has changed so let's look at some comparisons and explore how to migrate from the classic to the new.

Simplified Python API
^^^^^^^^^^^^^^^^^^^^^

One of the first big changes we've made is making simple use cases simpler. The only thing you need to create a minimal cluster is to give it a name.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster

    cluster = KubeCluster(name="mycluster")

The first step we see folks take in customising their clusters is to modify things like the container image, environment variables, resources, etc.
We've made all of the most common options available as keyword arguments to make small changes easier.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster

    cluster = KubeCluster(name="mycluster",
                          image='ghcr.io/dask/dask:latest',
                          n_workers=3
                          env={"FOO": "bar"},
                          resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}})

Advanced YAML API
^^^^^^^^^^^^^^^^^

We've taken care to simplify the API for new users, but we have also worked hard to ensure the new implementation provides even more
flexibility for advanced users.

Users of the classic implementation of ``KubeCluster`` have a lot of control over what the worker pods look like because you are required
to provide a full YAML ``Pod`` spec. Instead of creating a loose collection of ``Pod`` resources directly the new implementation groups everything together into a ``DaskCluster`` custom resource.
This resource contains some cluster configuration options and nested specs for the worker pods and scheduler pod/service.
This way things are infinitely configurable, just be careful not to shooot yourself in the foot.

The classic getting started page had the following pod spec example:

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

In the new implementation a cluster spec with the same options would look like this:

.. code-block:: yaml

    # cluster-spec.yml
    apiVersion: kubernetes.dask.org/v1
    kind: DaskCluster
    metadata:
      name: example
      labels:
        foo: bar
    spec:
      worker:
        replicas: 2
        spec:
          restartPolicy: Never
          containers:
          - name: worker
            image: "ghcr.io/dask/dask:latest"
            imagePullPolicy: "IfNotPresent"
            args: [dask-worker, --nthreads, '2', --no-dashboard, --memory-limit, 6GB, --death-timeout, '60', '--name', $(DASK_WORKER_NAME)]
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
      scheduler:
        spec:
          containers:
          - name: scheduler
            image: "ghcr.io/dask/dask:latest"
            imagePullPolicy: "IfNotPresent"
            args:
              - dask-scheduler
            ports:
              - name: tcp-comm
                containerPort: 8786
                protocol: TCP
              - name: http-dashboard
                containerPort: 8787
                protocol: TCP
            readinessProbe:
              httpGet:
                port: http-dashboard
                path: /health
              initialDelaySeconds: 5
              periodSeconds: 10
            livenessProbe:
              httpGet:
                port: http-dashboard
                path: /health
              initialDelaySeconds: 15
              periodSeconds: 20
        service:
          type: ClusterIP
          selector:
            dask.org/cluster-name: example
            dask.org/component: scheduler
          ports:
          - name: tcp-comm
            protocol: TCP
            port: 8786
            targetPort: "tcp-comm"
          - name: http-dashboard
            protocol: TCP
            port: 8787
            targetPort: "http-dashboard"

Note that the ``spec.worker.spec`` section of the new cluster spec matches the ``spec`` of the old pod spec. But as you can see there is a lot more configuration available in this example including first-class control over the scheduler pod and service.

One powerful difference of using our own custom resources is that *everything* about our cluster is contained in the ``DaskCluster`` spec and all of the cluster lifecycle logic is handled by our custom controller in Kubernetes.
This means we can equally create our cluster with Python or via the ``kubectl`` CLI.
You don't even need to have ``dask-kubernetes`` installed to manage your clusters if you have other Kubernetes tooling that you would like to integrate with natively.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster

    cluster = KubeCluster(custom_cluster_spec="cluster-spec.yml")

Is the same as:

.. code-block:: console

    $ kubectl apply -f cluster-spec.yml

You can still connect to the cluster created via ``kubectl`` back in Python by name and have all of the convenience of using a cluster manager object.

.. code-block:: python

    from dask.distributed import Client
    from dask_kubernetes.operator import KubeCluster

    cluster = KubeCluster.from_name("example")
    cluster.scale(5)
    client = Client(cluster)

Middle ground
^^^^^^^^^^^^^

There is also a middle ground for users who would prefer to stay in Python and have much of the spec generated for them, but still want to be able to make complex customisations.

When creating a new ``KubeCluster`` with keyword arguments those arguments are passed to a call to ``dask_kubernetes.operator.make_cluster_spec`` which is similar to ``dask_kubernetes.make_pod_spec`` that you may have used in the past.
This function generates a dictionary representation of your ``DaskCluster`` spec which you can modify and pass to ``KubeCluster`` yourself.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster, make_cluster_spec

    cluster = KubeCluster(name="foo", n_workers= 2, env={"FOO": "bar"})

    # is equivalent to

    spec = make_cluster_spec(name="foo", n_workers= 2, env={"FOO": "bar"})
    cluster = KubeCluster(custom_cluster_spec=spec)

This is useful if you want the convenience of keyword arguments for common options but still have the ability to make advanced tweaks like setting ``nodeSelector`` options on the worker pods.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster, make_cluster_spec

    spec = make_cluster_spec(name="selector-example", n_workers=2)
    spec["spec"]["worker"]["spec"]["nodeSelector"] = {"disktype": "ssd"}

    cluster = KubeCluster(custom_cluster_spec=spec)

This can also enable you to migrate smoothly over from the existing tooling if you are using ``make_pod_spec`` as the classic pod spec is a subset of the new cluster spec.

.. code-block:: python

    from dask_kubernetes.operator import KubeCluster, make_cluster_spec
    from dask_kubernetes.classic import make_pod_spec

    # generate your existing classic pod spec
    pod_spec = make_pod_spec(**your_custom_options)
    pod_spec[...] = ... # Your existing tweaks to the pod spec

    # generate a new cluster spec and merge in the existing pod spec
    cluster_spec = make_cluster_spec(name="merge-example")
    cluster_spec["spec"]["worker"]["spec"] = pod_spec["spec"]

    cluster = KubeCluster(custom_cluster_spec=cluster_spec)

Troubleshooting
---------------

Moving from the classic implementation to the new operator based implementation will require some effort on your part. Sorry about that.

Hopefully this guide has given you enough information that you are motivated and able to make the change.
However if you get stuck or you would like input from a Dask maintainer please don't hesitate to reach out to us via the `Dask Forum <https://dask.discourse.group/>`_.
