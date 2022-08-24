Custom Resources
================
.. currentmodule:: dask_kubernetes.experimental

.. warning::
    The Dask Operator for Kubernetes is experimental. So any `bug reports <https://github.com/dask/dask-kubernetes/issues>`_ are appreciated!

The Dask Operator has a few custom resources that can be used to create various Dask components.

- `DaskCluster`_ creates a full Dask cluster with a scheduler and workers.
- `DaskWorkerGroup`_ creates homogenous groups of workers, ``DaskCluster`` creates one by default but you can add more if you want multiple worker types.
- `DaskJob`_ creates a ``Pod`` that will run a script to completion along with a ``DaskCluster`` that the script can leverage.

DaskCluster
-----------

The ``DaskCluster`` custom resource creates a Dask cluster by creating a scheduler ``Pod``, scheduler ``Service`` and default `DaskWorkerGroup`_ which in turn creates worker ``Pod`` resources.

.. mermaid::

    graph TD
      DaskCluster(DaskCluster)
      SchedulerService(Scheduler Service)
      SchedulerPod(Scheduler Pod)
      DaskWorkerGroup(Default DaskWorkerGroup)
      WorkerPodA(Worker Pod A)
      WorkerPodB(Worker Pod B)
      WorkerPodC(Worker Pod C)

      DaskCluster --> SchedulerService
      DaskCluster --> SchedulerPod
      DaskCluster --> DaskWorkerGroup
      DaskWorkerGroup --> WorkerPodA
      DaskWorkerGroup --> WorkerPodB
      DaskWorkerGroup --> WorkerPodC

      classDef dask stroke:#FDA061,stroke-width:4px
      classDef dashed stroke-dasharray: 5 5

      class DaskCluster dask
      class DaskWorkerGroup dask
      class DaskWorkerGroup dashed
      class SchedulerService dashed
      class SchedulerPod dashed
      class WorkerPodA dashed
      class WorkerPodB dashed
      class WorkerPodC dashed

Let's create an example called ``cluster.yaml`` with the following configuration:

.. code-block:: yaml

    # cluster.yaml
    apiVersion: kubernetes.dask.org/v1
    kind: DaskCluster
    metadata:
      name: simple-cluster
    spec:
      worker:
        replicas: 2
        spec:
          containers:
          - name: worker
            image: "ghcr.io/dask/dask:latest"
            imagePullPolicy: "IfNotPresent"
            args:
              - dask-worker
              - --name
              - $(DASK_WORKER_NAME)
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
          type: NodePort
          selector:
            dask.org/cluster-name: simple-cluster
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
   NAME                     TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)             AGE
   simple-cluster-service   ClusterIP   10.96.85.120   <none>        8786/TCP,8787/TCP   86s

We can see here that port ``8786`` has been exposed for the Dask communication along with ``8787`` for the Dashboard.

How you access these service endpoints will `vary depending on your Kubernetes cluster configuration <https://kubernetes.io/docs/tasks/access-application-cluster/access-cluster-services/>`_.
For this quick example we could use ``kubectl`` to port forward the service to your local machine.

.. code-block:: console

   $ kubectl port-forward svc/simple-cluster-service 8786:8786
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

DaskWorkerGroup
---------------

When we create a ``DaskCluster`` resource a default worker group is created for us. But we can add more by creating more manifests. This allows us to create workers of different shapes and sizes that `Dask can leverage for different tasks <https://distributed.dask.org/en/stable/resources.html>`_.

.. mermaid::

    graph TD
      DaskCluster(DaskCluster)

      DefaultDaskWorkerGroup(Default DaskWorkerGroup)
      DefaultDaskWorkerPodA(Worker Pod A)
      DefaultDaskWorkerPodEllipsis(Worker Pod ...)

      HighMemDaskWorkerGroup(High Memory DaskWorkerGroup)
      HighMemDaskWorkerPodA(High Memory Worker Pod A)
      HighMemDaskWorkerPodEllipsis(High Memory Worker Pod ...)

      DaskCluster --> DefaultDaskWorkerGroup
      DefaultDaskWorkerGroup --> DefaultDaskWorkerPodA
      DefaultDaskWorkerGroup --> DefaultDaskWorkerPodEllipsis

      DaskCluster --> HighMemDaskWorkerGroup
      HighMemDaskWorkerGroup --> HighMemDaskWorkerPodA
      HighMemDaskWorkerGroup --> HighMemDaskWorkerPodEllipsis

      classDef dask stroke:#FDA061,stroke-width:4px
      classDef disabled stroke:#62636C
      classDef dashed stroke-dasharray: 5 5

      class DaskCluster disabled
      class DefaultDaskWorkerGroup disabled
      class DefaultDaskWorkerGroup dashed
      class DefaultDaskWorkerPodA dashed
      class DefaultDaskWorkerPodA disabled
      class DefaultDaskWorkerPodEllipsis dashed
      class DefaultDaskWorkerPodEllipsis disabled

      class HighMemDaskWorkerGroup dask
      class HighMemDaskWorkerPodA dashed
      class HighMemDaskWorkerPodEllipsis dashed


Let's create an example called ``highmemworkers.yaml`` with the following configuration:

.. code-block:: yaml

    # highmemworkers.yaml
    apiVersion: kubernetes.dask.org/v1
    kind: DaskWorkerGroup
    metadata:
      name: simple-cluster-highmem-worker-group
    spec:
      cluster: simple-cluster
      worker:
        replicas: 2
        spec:
          containers:
          - name: worker
            image: "ghcr.io/dask/dask:latest"
            imagePullPolicy: "IfNotPresent"
            resources:
              requests:
                memory: "32Gi"
              limits:
                memory: "32Gi"
            args:
              - dask-worker
              - --name
              - $(DASK_WORKER_NAME)
              - --resources
              - MEMORY=32e9

The main thing we need to ensure is that the ``cluster`` option matches the name of the cluster we created earlier. This will cause
the workers to join that cluster.

See the :ref:`config`. Now apply ``highmemworkers.yaml``

.. code-block:: console

   $ kubectl apply -f highmemworkers.yaml
   daskworkergroup.kubernetes.dask.org/simple-cluster-highmem-worker-group created

We can list our clusters:

.. code-block:: console

   $ kubectl get daskworkergroups
   NAME                                  AGE
   simple-cluster-default-worker-group   2 hours
   simple-cluster-highmem-worker-group   47s

We don't need to worry about deleting this worker group seperately, because it has joined the existing cluster Kubernetes will delete it
when the ``DaskCluster`` resource is deleted.

Scaling works the same was as the default worker group and can be done with the ``kubectl scale`` command.

DaskJob
-------

The ``DaskJob`` custom resource behaves similarly to other Kubernetes batch resources.
It creates a ``Pod`` that executes a command to completion. The difference is that the ``DaskJob`` also creates
a ``DaskCluster`` alongside it and injects the appropriate configuration into the job ``Pod`` for it to
automatically connect to and leverage the Dask cluster.

.. mermaid::

    graph TD
      DaskJob(DaskJob)
      DaskCluster(DaskCluster)
      SchedulerService(Scheduler Service)
      SchedulerPod(Scheduler Pod)
      DaskWorkerGroup(Default DaskWorkerGroup)
      WorkerPodA(Worker Pod A)
      WorkerPodB(Worker Pod B)
      WorkerPodC(Worker Pod C)
      JobPod(Job Runner Pod)

      DaskJob --> DaskCluster
      DaskJob --> JobPod
      DaskCluster --> SchedulerService
      SchedulerService --> SchedulerPod
      DaskCluster --> DaskWorkerGroup
      DaskWorkerGroup --> WorkerPodA
      DaskWorkerGroup --> WorkerPodB
      DaskWorkerGroup --> WorkerPodC

      classDef dask stroke:#FDA061,stroke-width:4px
      classDef dashed stroke-dasharray: 5 5
      class DaskJob dask
      class DaskCluster dask
      class DaskCluster dashed
      class DaskWorkerGroup dask
      class DaskWorkerGroup dashed
      class SchedulerService dashed
      class SchedulerPod dashed
      class WorkerPodA dashed
      class WorkerPodB dashed
      class WorkerPodC dashed
      class JobPod dashed

Let's create an example called ``job.yaml`` with the following configuration:

.. code-block:: yaml

    # job.yaml
    apiVersion: kubernetes.dask.org/v1
    kind: DaskJob
    metadata:
      name: simple-job
      namespace: default
    spec:
      job:
        spec:
          containers:
            - name: job
              image: "ghcr.io/dask/dask:latest"
              imagePullPolicy: "IfNotPresent"
              args:
                - python
                - -c
                - "from dask.distributed import Client; client = Client(); # Do some work..."

      cluster:
        spec:
          worker:
            replicas: 2
            spec:
              containers:
                - name: worker
                  image: "ghcr.io/dask/dask:latest"
                  imagePullPolicy: "IfNotPresent"
                  args:
                    - dask-worker
                    - --name
                    - $(DASK_WORKER_NAME)
                  env:
                    - name: WORKER_ENV
                      value: hello-world # We dont test the value, just the name
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
                  env:
                    - name: SCHEDULER_ENV
                      value: hello-world
            service:
              type: ClusterIP
              selector:
                dask.org/cluster-name: simple-job-cluster
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


Editing this file will change the default configuration of you Dask job. See the :ref:`config`. Now apply ``job.yaml``

.. code-block:: console

    $ kubectl apply -f job.yaml
      daskjob.kubernetes.dask.org/simple-job created

Now if we check our cluster resources we should see our job and cluster pods being created.

.. code-block:: console

    $ kubectl get pods
    NAME                                                        READY   STATUS              RESTARTS   AGE
    simple-job-cluster-scheduler                                1/1     Running             0          8s
    simple-job-runner                                           1/1     Running             0          8s
    simple-job-cluster-default-worker-group-worker-1f6c670fba   1/1     Running             0          8s
    simple-job-cluster-default-worker-group-worker-791f93d9ec   1/1     Running             0          8s

Our runner pod will be doing whatever we configured it to do. In our example you can see we just create a simple ``dask.distributed.Client`` object like this:

.. code-block:: python

    from dask.distributed import Client


    client = Client()

    # Do some work...


We can do this because the job pod gets some additional environment variables set at runtime which tell the ``Client`` how to connect to the cluster, so the user doesn't need to
worry about it.

The job pod has a default restart policy of ``OnFalure`` so if it exits with anything other than a ``0`` return code it will be restarted automatically until it completes successfully. When it does return a ``0`` it will
go into a ``Completed`` state and the Dask cluster will be cleaned up automatically freeing up Kubernetes cluster resources.

.. code-block:: console

    $ kubectl get pods
    NAME                                                        READY   STATUS              RESTARTS   AGE
    simple-job-runner                                           0/1     Completed           0          14s
    simple-job-cluster-scheduler                                1/1     Terminating         0          14s
    simple-job-cluster-default-worker-group-worker-1f6c670fba   1/1     Terminating         0          14s
    simple-job-cluster-default-worker-group-worker-791f93d9ec   1/1     Terminating         0          14s

When you delete the ``DaskJob`` resource everything is delete automatically, whether that's just the ``Completed`` runner pod left over after a successful run or a full Dask cluster and runner that is still running.

.. code-block:: console

    $ kubectl delete -f job.yaml
    daskjob.kubernetes.dask.org "simple-job" deleted

.. _config:

DaskAutoscaler
--------------

The ``DaskAutoscaler`` resource allows the scheduler to scale up and down the number of workers
using dask's adaptive mode.

By creating the resource the operator controller will periodically poll the scheduler and request the desired number of workers.
The scheduler calculates this number by profiling the tasks it is processing and then extrapolating how many workers it would need
to complete the current graph within 5 seconds.

The controller will constrain this number between the ``minimum`` and ``maximum`` values configured in the ``DaskAutoscaler`` resource
and then update the number of replicas in the default ``DaskWorkerGroup``.


.. code-block:: yaml

    # autoscaler.yaml
    apiVersion: kubernetes.dask.org/v1
    kind: DaskAutoscaler
    metadata:
      name: simple-cluster-autoscaler
    spec:
      cluster: "simple-cluster"
      minimum: 1  # we recommend always having a minimum of 1 worker so that an idle cluster can start working on tasks immediately
      maximum: 10 # you can place a hard limit on the number of workers regardless of what the scheduler requests

.. code-block:: console

    $ kubectl apply -f autoscaler.yaml
    daskautoscaler.kubernetes.dask.org/simple-cluster-autoscaler created

You can end the autoscaling at any time by deleting the resource. The number of workers will remain at whatever the autoscaler last
set it to.

.. code-block:: console

    $ kubectl delete -f autoscaler.yaml
    daskautoscaler.kubernetes.dask.org/simple-cluster-autoscaler deleted

.. note::

    The autoscaler will only scale the default ``WorkerGroup``. If you have additional worker groups configured they
    will not be taken into account.

Full Configuration Reference
----------------------------

Full ``DaskCluster`` spec reference.

.. code-block:: yaml

    apiVersion: kubernetes.dask.org/v1
    kind: DaskCluster
    metadata:
      name: example
    spec:
      worker:
        replicas: 2 # number of replica workers to spawn
        spec: ... # PodSpec, standard k8s pod - https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#podspec-v1-core
      scheduler:
        spec: ... # PodSpec, standard k8s pod - https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#podspec-v1-core
        service: ... # ServiceSpec, standard k8s service - https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#servicespec-v1-core

Full ``DaskWorkerGroup`` spec reference.

.. code-block:: yaml

    apiVersion: kubernetes.dask.org/v1
    kind: DaskWorkerGroup
    metadata:
      name: example
    spec:
      cluster: "name of DaskCluster to associate worker group with"
      worker:
        replicas: 2 # number of replica workers to spawn
        spec: ... # PodSpec, standard k8s pod - https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#podspec-v1-core

Full ``DaskJob`` spec reference.

.. code-block:: yaml

    apiVersion: kubernetes.dask.org/v1
    kind: DaskJob
    metadata:
      name: example
    spec:
      job:
        spec: ... # PodSpec, standard k8s pod - https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#podspec-v1-core
      cluster:
        spec: ... # ClusterSpec, DaskCluster resource spec

Full ``DaskAutoscaler`` spec reference.

.. code-block:: yaml

    apiVersion: kubernetes.dask.org/v1
    kind: DaskAutoscaler
    metadata:
      name: example
    spec:
      cluster: "name of DaskCluster to autoscale"
      minimum: 0  # minimum number of workers to create
      maximum: 10 # maximum number of workers to create
