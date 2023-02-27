aiopykube
=========

Throughout the lifetime of this library we have used almost every Python Kubernetes library available.
They all seem to have their own drawbacks but my favourite for clean code and readibility is `pykube-ng <https://pykube.readthedocs.io/en/latest/>`_.

However, ``pykube`` is built on ``requests`` and doesn't natively support ``asyncio`` which we heavily use in Dask. To make it a little easier to work with we have a
``dask_kubernetes.aiopykube`` submodule which provides an async shim that has the same API as ``pykube`` and runs all IO calls in a threadpool executor.

The shim also includes a ``dask_kubernetes.aiopykube.dask`` submodule with custom objects to represent :doc:`Dask custom resources <operator_resources>`.

Examples
--------

Iterate over Pods
^^^^^^^^^^^^^^^^^

.. code-block:: python

    from dask_kubernetes.aiopykube import HTTPClient, KubeConfig
    from dask_kubernetes.aiopykube.objects import Pod

    api = HTTPClient(KubeConfig.from_env())

    async for pod in Pod.objects(namespace="default"):
        # Do something

Create a new Pod
^^^^^^^^^^^^^^^^

.. code-block:: python

    from dask_kubernetes.aiopykube import HTTPClient, KubeConfig
    from dask_kubernetes.aiopykube.objects import Pod

    api = HTTPClient(KubeConfig.from_env())

    pod = Pod(
        api,
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": name},
            "spec": {
                "containers": [
                    {"name": "pause", "image": "gcr.io/google_containers/pause"}
                ]
            },
        },
    )

    await pod.create()

Scale an existing deployment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from dask_kubernetes.aiopykube import HTTPClient, KubeConfig
    from dask_kubernetes.aiopykube.objects import Deployment

    api = HTTPClient(KubeConfig.from_env())

    deployment = await Deployment.objects(api).get_by_name("mydeployment")
    await deployment.scale(5)

Delete a DaskCluster custom resource
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from dask_kubernetes.aiopykube import HTTPClient, KubeConfig
    from dask_kubernetes.aiopykube.dask import DaskCluster

    api = HTTPClient(KubeConfig.from_env())
    cluster = await DaskCluster.objects(api, namespace=namespace).get_by_name("mycluster")
    await cluster.delete()

API
---

dask_kubernetes.aiopykube.query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
   dask_kubernetes.aiopykube.query.Query
   dask_kubernetes.aiopykube.query.WatchQuery

Query
*****

.. autoclass:: dask_kubernetes.aiopykube.query.Query
   :members:

WatchQuery
**********

.. autoclass:: dask_kubernetes.aiopykube.query.WatchQuery
   :members:

dask_kubernetes.aiopykube.dask
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
    dask_kubernetes.aiopykube.dask.DaskCluster
    dask_kubernetes.aiopykube.dask.DaskWorkerGroup
    dask_kubernetes.aiopykube.dask.DaskJob
    dask_kubernetes.aiopykube.dask.DaskAutoscaler

DaskCluster
***********

.. autoclass:: dask_kubernetes.aiopykube.dask.DaskCluster
   :members:

DaskWorkerGroup
***************

.. autoclass:: dask_kubernetes.aiopykube.dask.DaskWorkerGroup
   :members:

DaskJob
*******

.. autoclass:: dask_kubernetes.aiopykube.dask.DaskJob
   :members:

DaskAutoscaler
**************

.. autoclass:: dask_kubernetes.aiopykube.dask.DaskAutoscaler
   :members:

dask_kubernetes.aiopykube.objects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autosummary::
    dask_kubernetes.aiopykube.objects.ConfigMap
    dask_kubernetes.aiopykube.objects.CronJob
    dask_kubernetes.aiopykube.objects.DaemonSet
    dask_kubernetes.aiopykube.objects.Deployment
    dask_kubernetes.aiopykube.objects.Endpoint
    dask_kubernetes.aiopykube.objects.Event
    dask_kubernetes.aiopykube.objects.LimitRange
    dask_kubernetes.aiopykube.objects.ResourceQuota
    dask_kubernetes.aiopykube.objects.ServiceAccount
    dask_kubernetes.aiopykube.objects.Ingress
    dask_kubernetes.aiopykube.objects.Job
    dask_kubernetes.aiopykube.objects.Namespace
    dask_kubernetes.aiopykube.objects.Node
    dask_kubernetes.aiopykube.objects.Pod
    dask_kubernetes.aiopykube.objects.ReplicationController
    dask_kubernetes.aiopykube.objects.ReplicaSet
    dask_kubernetes.aiopykube.objects.Secret
    dask_kubernetes.aiopykube.objects.Service
    dask_kubernetes.aiopykube.objects.PersistentVolume
    dask_kubernetes.aiopykube.objects.PersistentVolumeClaim
    dask_kubernetes.aiopykube.objects.HorizontalPodAutoscaler
    dask_kubernetes.aiopykube.objects.StatefulSet
    dask_kubernetes.aiopykube.objects.Role
    dask_kubernetes.aiopykube.objects.RoleBinding
    dask_kubernetes.aiopykube.objects.ClusterRole
    dask_kubernetes.aiopykube.objects.ClusterRoleBinding
    dask_kubernetes.aiopykube.objects.PodSecurityPolicy
    dask_kubernetes.aiopykube.objects.PodDisruptionBudget
    dask_kubernetes.aiopykube.objects.CustomResourceDefinition

ConfigMap
*********

.. autoclass:: dask_kubernetes.aiopykube.objects.ConfigMap
   :members:

CronJob
*******

.. autoclass:: dask_kubernetes.aiopykube.objects.CronJob
   :members:

DaemonSet
*********

.. autoclass:: dask_kubernetes.aiopykube.objects.DaemonSet
   :members:

Deployment
**********

.. autoclass:: dask_kubernetes.aiopykube.objects.Deployment
   :members:

Endpoint
********

.. autoclass:: dask_kubernetes.aiopykube.objects.Endpoint
   :members:

Event
*****

.. autoclass:: dask_kubernetes.aiopykube.objects.Event
   :members:

LimitRange
**********

.. autoclass:: dask_kubernetes.aiopykube.objects.LimitRange
   :members:

ResourceQuota
*************

.. autoclass:: dask_kubernetes.aiopykube.objects.ResourceQuota
   :members:

ServiceAccount
**************

.. autoclass:: dask_kubernetes.aiopykube.objects.ServiceAccount
   :members:

Ingress
*******

.. autoclass:: dask_kubernetes.aiopykube.objects.Ingress
   :members:

Job
***

.. autoclass:: dask_kubernetes.aiopykube.objects.Job
   :members:

Namespace
*********

.. autoclass:: dask_kubernetes.aiopykube.objects.Namespace
   :members:

Node
****

.. autoclass:: dask_kubernetes.aiopykube.objects.Node
   :members:

Pod
***

.. autoclass:: dask_kubernetes.aiopykube.objects.Pod
   :members:

ReplicationController
*********************

.. autoclass:: dask_kubernetes.aiopykube.objects.ReplicationController
   :members:

ReplicaSet
**********

.. autoclass:: dask_kubernetes.aiopykube.objects.ReplicaSet
   :members:

Secret
******

.. autoclass:: dask_kubernetes.aiopykube.objects.Secret
   :members:

Service
*******

.. autoclass:: dask_kubernetes.aiopykube.objects.Service
   :members:

PersistentVolume
****************

.. autoclass:: dask_kubernetes.aiopykube.objects.PersistentVolume
   :members:

PersistentVolumeClaim
*********************

.. autoclass:: dask_kubernetes.aiopykube.objects.PersistentVolumeClaim
   :members:

HorizontalPodAutoscaler
***********************

.. autoclass:: dask_kubernetes.aiopykube.objects.HorizontalPodAutoscaler
   :members:

StatefulSet
***********

.. autoclass:: dask_kubernetes.aiopykube.objects.StatefulSet
   :members:

Role
****

.. autoclass:: dask_kubernetes.aiopykube.objects.Role
   :members:

RoleBinding
***********

.. autoclass:: dask_kubernetes.aiopykube.objects.RoleBinding
   :members:

ClusterRole
***********

.. autoclass:: dask_kubernetes.aiopykube.objects.ClusterRole
   :members:

ClusterRoleBinding
******************

.. autoclass:: dask_kubernetes.aiopykube.objects.ClusterRoleBinding
   :members:

PodSecurityPolicy
*****************

.. autoclass:: dask_kubernetes.aiopykube.objects.PodSecurityPolicy
   :members:

PodDisruptionBudget
*******************

.. autoclass:: dask_kubernetes.aiopykube.objects.PodDisruptionBudget
   :members:

CustomResourceDefinition
************************

.. autoclass:: dask_kubernetes.aiopykube.objects.CustomResourceDefinition
   :members:

FAQ
---

Why roll our own wrapper?
^^^^^^^^^^^^^^^^^^^^^^^^^

There does appear to be an ``aiopykube`` package on PyPI but it hasn't been updated for a long time and doesn't link to any source on GitHub or other source repository.
This probably fills the same role but we're apprehensive to depend on it in this state.

Why not release this is a separate package?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In theory we could pull the implementation out of ``dask-kubernetes`` into another dependency. If there is demand from the community to do this we could definitely consider it.

Why not use ``kubernetes``, ``kubernetes-asyncio``, ``aiokubernetes``, etc?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The most popular Kubernetes libraries for Python are swagger generated SDKs based on the Kubernetes API spec.
This results in libraries that do not feel Pythonic and only have complex reference documentation without any explaination, how-to or tutorial content.

It's true that ``pykube`` is also a little lacking in documentation, however the code is simple enough and written by a human so code-spelunking isn't too unpleasant.

Why use a threadpool executor instead of ``aiohttp``?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The way ``pykube`` leverages ``requests`` has a lot of status checking and error handling (which is great). However, this would mean rewriting far more of the code in our shim if we
were to switch out the underlying IO library. To try and keep the shim as lightweight as possible we've simply wrapped all methods that make blocking IO calls in calls to ``run_in_executor``.

If we were to ever spin this shim out into a separate package we would probably advocate for a deeper rewrite using ``aiohttp`` instead of ``requests``.

Why are some methods not implemented?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

There are a couple of design decisions in ``pykube`` which have limited how much of it can be converted to ``asyncio``.
There are a few properties, such as ``pukube.query.Query.query_cache``, that make HTTP calls. Using any kind of IO in a property or dunder like ``__len__`` is generally frowned upon
and therefore there is no ``asyncio`` way to make or call a property asynchronously.

Instead of changing the API we've set these to raise ``NotImplementedError`` with useful exceptions that suggest an alternative way to achieve the same thing.
