Dask Operator
=============
.. currentmodule:: dask_kubernetes.experimental

.. warning::
    The Dask Operator for Kubernetes is experimental. So any `bug reports <https://github.com/dask/dask-kubernetes/issues>`_ are appreciated!

The Dask Operator is a small service that runs on you Kubernetes cluster and allows you to create and manage your Dask clusters as native Kubernetes resources.
Creating clusters can either be done via the Kubernetes API (``kubectl``) or the Python API (:class:`dask_kubernetes.experimental.KubeCluster`)

Installing the Operator
-----------------------


To install the the operator first we need to create the Dask custom resources:

.. code-block:: console

   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/customresources/daskcluster.yaml
   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/customresources/daskworkergroup.yaml

Then you should be able to list your Dask clusters via ``kubectl``.

.. code-block:: console

   $ kubectl get daskclusters
   No resources found in default namespace.

Next we need to install the operator. The operator will watch for new ``daskcluster`` resources being created and add/remove pods/services/etc to create the cluster.

.. code-block:: console

   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/deployment/manifest.yaml

This will create the appropriate roles, service accounts and a deployment for the operator. We can check the operator pod is running:

.. code-block:: console

   $ kubectl get pods -A -l application=dask-kubernetes-operator
   NAMESPACE     NAME                                        READY   STATUS    RESTARTS   AGE
   kube-system   dask-kubernetes-operator-775b8bbbd5-zdrf7   1/1     Running   0          74s


Creating a Dask cluster via ``kubectl``
---------------------------------------

Now we can create Dask clusters.

Let's create an example called ``cluster.yaml`` with the following configuration:

.. code-block:: yaml

   # cluster.yaml
   apiVersion: kubernetes.dask.org/v1
   kind: DaskCluster
   metadata:
     name: simple-cluster
   spec:
     image: "ghcr.io/dask/dask:latest"
     replicas: 3

Editing this file will change the default configuration of you Dask cluster. See the Configuration Reference :ref:`config`. Now apply ``cluster.yaml``

.. code-block:: console

   $ kubectl apply -f cluster.yaml
   daskcluster.kubernetes.dask.org/simple-cluster created

We can list our clusters:

.. code-block:: console

   $ kubectl get daskclusters
   NAME             AGE
   simple-cluster   47s

To connect to this Dask cluster we can use the service that was created for us.

.. code-block:: console

   $ kubectl get svc -l dask.org/cluster-name=simple-cluster
   NAME             TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
   simple-cluster   ClusterIP   10.96.85.120   <none>        8786/TCP,8787/TCP   86s

We can see here that port ``8786`` has been exposed for the Dask communication along with ``8787`` for the Dashboard.

How you access these service endpoints will `vary depending on your Kubernetes cluster configuration <https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster-services/>`_.
For this quick example we could use ``kubectl`` to port forward the service to your local machine.

.. code-block:: console

   $ kubectl port-forward svc/simple-cluster 8786:8786
   Forwarding from 127.0.0.1:8786 -> 8786
   Forwarding from [::1]:8786 -> 8786

Then we can connect to it from a Python session.

.. code-block:: python

   >>> from dask.distributed import Client
   >>> client = Client("localhost:8786")
   >>> print(client)
   <Client: 'tcp://10.244.0.12:8786' processes=3 threads=12, memory=23.33 GiB>

We can also list all of the pods created by the operator to run our cluster.

.. code-block:: console

   $ kubectl get po -l dask.org/cluster-name=simple-cluster
   NAME                                                                          READY   STATUS    RESTARTS   AGE
   simple-cluster-default-worker-group-worker-13f4f0d13bbc40a58cfb81eb374f26c3   1/1     Running   0          104s
   simple-cluster-default-worker-group-worker-aa79dfae83264321a79f1f0ffe91f700   1/1     Running   0          104s
   simple-cluster-default-worker-group-worker-f13c4f2103e14c2d86c1b272cd138fe6   1/1     Running   0          104s
   simple-cluster-scheduler                                                      1/1     Running   0          104s

The workers we see here are created by our clusters default ``workergroup`` resource that was also created by the operator.

You can scale the ``workergroup`` like you would a ``Deployment`` or ``ReplicaSet``:

.. code-block:: console

   $ kubectl scale --replicas=5 daskworkergroup simple-cluster-default-worker-group
   daskworkergroup.kubernetes.dask.org/simple-cluster-default-worker-group scaled

We can verify that new pods have been created.

.. code-block:: console

   $ kubectl get po -l dask.org/cluster-name=simple-cluster
   NAME                                                                          READY   STATUS    RESTARTS   AGE
   simple-cluster-default-worker-group-worker-13f4f0d13bbc40a58cfb81eb374f26c3   1/1     Running   0          5m26s
   simple-cluster-default-worker-group-worker-a52bf313590f432d9dc7395875583b52   1/1     Running   0          27s
   simple-cluster-default-worker-group-worker-aa79dfae83264321a79f1f0ffe91f700   1/1     Running   0          5m26s
   simple-cluster-default-worker-group-worker-f13c4f2103e14c2d86c1b272cd138fe6   1/1     Running   0          5m26s
   simple-cluster-default-worker-group-worker-f4223a45b49d49288195c540c32f0fc0   1/1     Running   0          27s
   simple-cluster-scheduler                                                      1/1     Running   0          5m26s

Finally we can delete the cluster either by deleting the manifest we applied before, or directly by name:

.. code-block:: console

   $ kubectl delete -f cluster.yaml
   daskcluster.kubernetes.dask.org "simple-cluster" deleted

   $ kubectl delete daskcluster simple-cluster
   daskcluster.kubernetes.dask.org "simple-cluster" deleted

Creating a Dask cluster via the cluster manager
-----------------------------------------------

Alternatively, with the cluster manager, you can conveniently create and manage a Dask cluster in Python. Then connect a :class:`dask.distributed.Client` object to it directly and perform your work.

Under the hood the Python cluster manager will interact with ther Kubernetes API to create resources for us as we did above.

To create a cluster in the default namespace, run the following

.. code-block:: python

   from dask_kubernetes.experimental import KubeCluster

   cluster = KubeCluster(name='foo')

You can change the default configuration of the cluster by passing additional args
to the python class (``namespace``, ``n_workers``, etc.) of your cluster. See the API refernce :ref:`api`

You can scale the cluster

.. code-block:: python

   # Scale up the cluster
   cluster.scale(5)

   # Scale down the cluster
   cluster.scale(1)

You can connect to the client

.. code-block:: python

    from dask.distributed import Client

    # Connect Dask to the cluster
    client = Client(cluster)

Finally delete the cluster by running

.. code-block:: python

   cluster.close()

.. _config:

Additional Worker Groups
------------------------

The operator also has support for creating additional worker groups. These are extra groups of workers with different
configuration settings and can be scaled separately. You can then use `resource annotations <https://distributed.dask.org/en/stable/resources.html>`_`
to schedule different tasks to different groups.

For example you may wish to have a smaller pool of workers that have more memory for memory intensive tasks, or GPUs for compute intensive tasks.


Creating a Worker Group via ``kubectl``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When we create a ``DaskCluster`` resource a default worker group is created for us. But we can add more by creating more manifests.

Let's create an example called ``highmemworkers.yaml`` with the following configuration:

.. code-block:: yaml

   # highmemworkers.yaml
   apiVersion: kubernetes.dask.org/v1
   kind: DaskWorkerGroup
   metadata:
      name: simple-cluster-highmem-worker-group
   spec:
      cluster: simple-cluster
      imagePullSecrets: null
      image: "ghcr.io/dask/dask:latest"
      imagePullPolicy: "IfNotPresent"
      replicas: 2
      resources:
         requests:
            memory: "2Gi"
         limits:
            memory: "64Gi"
      env: []

The main thing we need to ensure is that the ``cluster`` option matches the name of the cluster we created earlier. This will cause
the workers to join that cluster.

See the Configuration Reference :ref:`config`. Now apply ``highmemworkers.yaml``

.. code-block:: console

   $ kubectl apply -f highmemworkers.yaml
   daskworkergroup.kubernetes.dask.org/simple-cluster-highmem-worker-group created

We can list our clusters:

.. code-block:: console

   $ kubectl get daskworkergroups
   NAME                                  AGE
   simple-cluster-default-worker-group   2 hours
   simple-cluster-highmem-worker-group   47s

We don't need to worry about deleting this worker group seperately, because it has joined the existing cluster it will be deleted
when the ``DaskCluster`` resource is deleted.

Scaling works the same was as the default worker group and can be done with the ``kubectl scale`` command.

Creating an additional worker group via the cluster manager
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Additional worker groups can also be created via the cluster manager in Python.

.. code-block:: python

   from dask_kubernetes.experimental import KubeCluster

   cluster = KubeCluster(name='foo')

   cluster.add_worker_group(name="highmem", n_workers=2, resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}})

We can also scale the worker groups by name from the cluster object.

.. code-block:: python

   cluster.scale(5, worker_group="highmem")

Additional worker groups can also be deleted in Python.

.. code-block:: python

   cluster.delete_worker_group(name="highmem")

Any additional worker groups you create will be deleted when the cluster is deleted.

Configuration Reference
-----------------------

Full ``DaskCluster`` spec reference.

.. code-block:: yaml

   apiVersion: kubernetes.dask.org/v1
   kind: DaskCluster
   metadata:
     name: example

   spec:
     # imagePullSecrets to be passed to the scheduler and worker pods
     imagePullSecrets: null

     # image to be used by the scheduler and workers, should contain a Python environment that matches where you are connecting your Client
     image: "ghcr.io/dask/dask:latest"

     # imagePullPolicy to be passed to scheduler and worker pods
     imagePullPolicy: "IfNotPresent"

     # Dask communication protocol to use
     protocol: "tcp"

     # Number of Dask worker replicas to create in the default worker group
     replicas: 3

     # Hardware resources to be set on the worker pods
     resources: {}

     # Environment variables to be set on the worker pods
     env: []

     # Scheduler specific options
     scheduler:

       # Hardware resources to be set on the scheduler pod (if omitted will use worker setting)
       resources: {}

       # Environment variables to be set on the scheduler pod (if omitted will use worker setting)
       env: []

       # Service type to use for exposing the scheduler
       serviceType: "ClusterIP"

.. _api:

API
---

.. currentmodule:: dask_kubernetes.experimental

.. autosummary::
   KubeCluster
   KubeCluster.scale
   KubeCluster.get_logs
   KubeCluster.add_worker_group
   KubeCluster.delete_worker_group
   KubeCluster.close

.. autoclass:: KubeCluster
   :members:
