Overview
========
.. currentmodule:: dask_kubernetes.operator

What is the operator?
---------------------

The Dask Operator is a small service that runs on your Kubernetes cluster and allows you to create and manage your Dask clusters as Kubernetes resources.
Creating clusters can either be done via the :doc:`Kubernetes API with kubectl <operator_resources>` or the :doc:`Python API with KubeCluster <operator_kubecluster>`.

To :doc:`install the operator <operator_installation>` you need to apply some custom resource definitions that allow us to describe Dask resources and the operator itself which is a small Python application that
watches the Kubernetes API for events related to our custom resources and creates other resources such as ``Pods`` and ``Services`` accordingly.

What resources does the operator manage?
---------------------------------------

The operator manages a hierarchy of resources, some custom resources to represent Dask primitives like clusters and worker groups, and native Kubernetes resources such as pods and services to run the cluster processes and facilitate communication.

.. mermaid::

    graph TD
      DaskJob(DaskJob)
      DaskCluster(DaskCluster)
      DaskAutoscaler(DaskAutoscaler)
      SchedulerService(Scheduler Service)
      SchedulerPod(Scheduler Pod)
      DaskWorkerGroup(DaskWorkerGroup)
      WorkerPodA(Worker Pod A)
      WorkerPodB(Worker Pod B)
      WorkerPodC(Worker Pod C)
      JobPod(Job Runner Pod)

      DaskJob --> DaskCluster
      DaskJob --> JobPod
      DaskCluster --> SchedulerService
      DaskCluster --> DaskAutoscaler
      SchedulerService --> SchedulerPod
      DaskCluster --> DaskWorkerGroup
      DaskWorkerGroup --> WorkerPodA
      DaskWorkerGroup --> WorkerPodB
      DaskWorkerGroup --> WorkerPodC

      classDef dask stroke:#FDA061,stroke-width:4px
      classDef dashed stroke-dasharray: 5 5
      class DaskJob dask
      class DaskCluster dask
      class DaskWorkerGroup dask
      class DaskAutoscaler dask
      class DaskAutoscaler dashed
      class SchedulerService dashed
      class SchedulerPod dashed
      class WorkerPodA dashed
      class WorkerPodB dashed
      class WorkerPodC dashed
      class JobPod dashed


Worker Groups
^^^^^^^^^^^^^

A ``DaskWorkerGroup`` represents a homogenous group of workers that can be scaled. The resource is similar to a native Kubernetes ``Deployment`` in that it manages a group of workers
with some intelligence around the ``Pod`` lifecycle. A worker group must be attached to a Dask Cluster resource in order to function.

All `Kubernetes annotations <https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/>` on the
``DaskWorkerGroup`` resource will be passed onto worker ``Pod`` resources. Annotations created by `kopf` or
`kubectl` (i.e. starting with "kopf.zalando.org" or "kubectl.kubernetes.io") will not be passed on.


Clusters
^^^^^^^^

The ``DaskCluster`` custom resource creates a Dask cluster by creating a scheduler ``Pod``, scheduler ``Service`` and default ``DaskWorkerGroup`` which in turn creates worker ``Pod`` resources.

Workers connect to the scheduler via the scheduler ``Service`` and that service can also be exposed to the user in order to connect clients and perform work.

The operator also has support for creating additional worker groups. These are extra groups of workers with different
configuration settings and can be scaled separately. You can then use `resource annotations <https://distributed.dask.org/en/stable/resources.html>`_
to schedule different tasks to different groups.

All `Kubernetes annotations <https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/>` on the
``DaskCluster`` resource will be passed onto the scheduler ``Pod`` as well the ``DaskWorkerGroup`` resources.
Annotations created by `kopf` or `kubectl` (i.e. starting with "kopf.zalando.org" or "kubectl.kubernetes.io") will not
be passed on.

For example you may wish to have a smaller pool of workers that have more memory for memory intensive tasks, or GPUs for compute intensive tasks.

Jobs
^^^^

A ``DaskJob`` is a batch style resource that creates a ``Pod`` to perform some specific task from start to finish alongside a ``DaskCluster`` that can be leveraged to perform the work.

All `Kubernetes annotations <https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations/>` on the
``DaskJob`` resource will be passed on to the job-runner ``Pod`` resource. If one also wants to set Kubernetes
annotations on the cluster-related resources (scheduler and worker ``Pods``), these can be set as
``spec.cluster.metadata`` in the ``DaskJob`` resource. Annotations created by `kopf` or `kubectl` (i.e. starting with
"kopf.zalando.org" or "kubectl.kubernetes.io") will not be passed on.

Once the job ``Pod`` runs to completion the cluster is removed automatically to save resources. This is great for workflows like training a distributed machine learning model with Dask.

Autoscalers
^^^^^^^^^^^

A ``DaskAutoscaler`` resource will communicate with the scheduler periodically and auto scale the default ``DaskWorkerGroup`` to the desired number of workers.
