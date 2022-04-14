Dask Operator
===========

.. warning::
    `KubeCluster2` is experimental for now. So any bug reports are appreciated!

:doc: The Dask Operator is for creating and managing a Dask Cluster. It's installed on the Kubernetes cluster and then users can create clusters via the Kubernetes API (``kubectl``) or the Python API (``KubeCluster2``)

Installing the Operator
-----------------------

.. currentmodule:: dask_kubernetes

First you must install the Dask Operator and have access to a Kubernetes Cluster. To install the the operator, you'll need to deploy the custom resources

.. code-block:: bash

   kubectl apply -f dask_kubernetes/operator/customresources/daskcluster.yaml
   kubectl apply -f dask_kubernetes/operator/customresources/daskworkergroup.yaml

Then, you'll need to create the operator

.. code-block:: bash
   kubectl apply -f dask_kubernetes/operator/deployment/manifest.yaml
   
Creating a Dask cluster via `kubectl`
-------------------------------------

To create a cluster in the default namespace, run the following

Create a file called `cluster.yaml` and provide it with the following configuration

.. code-block:: yaml

   apiVersion: kubernetes.dask.org/v1
   kind: DaskCluster
   metadata:
     name: simple-cluster
   spec:
     imagePullSecrets: null
     image: "daskdev/dask:latest"
     imagePullPolicy: "IfNotPresent"
     protocol: "tcp"
     scheduler:
       resources: {}
       env: {}
       serviceType: "ClusterIP"
     replicas: 3
     resources: {}
     env: {}

Editing this file will change the default configuration of you Dask cluster. Now deploy `cluster.yaml`

.. code-block:: bash

   kubectl apply -f <path to cluster.yaml>

You can scale the cluster

.. code-block:: bash

   kubectl scale --replicas=5 daskworkergroup simple-cluster-default-worker-group

Finally delete the cluster by running

.. code-block:: bash

   kubectl delete -f <path to cluster.yaml>

Creating a Dask cluster via the cluster manager 
-----------------------------------------------

 With the cluster object, you can conveniently create and manage a Dask cluster. And connect a :class:`dask.distributed.Client` object to the cluster and perform your work.

To create a cluster in the default namespace, run the following

.. code-block:: python

   cluster = KubeCluster2(name='foo')

You can change the default configuration of the cluster by passing additional args
to the python class (`namespace`, `n_workers`, etc.) of your cluster.

You can scale the cluster

.. code-block:: python

   # Scale up the cluster
   cluster.scale(5)

   # Scale down the cluster 
   cluster.scale(1)

Or add an additional worker group

.. code-block:: python

   cluster.add_worker_group("additional")

   # Scale the new worker group
   cluster.scale(5, "additional")

You can connect to the client

.. code-block:: python

    # Example usage
    from dask.distributed import Client
    import dask.array as da

    # Connect Dask to the cluster
    client = Client(cluster)

    # Create a large array and calculate the mean
    array = da.ones((1000, 1000, 1000))
    print(array.mean().compute())  # Should print 1.0

Finally delete the cluster by running

.. code-block:: python

   cluster.close()

API
---

.. currentmodule:: dask_kubernetes

.. autosummary::
   KubeCluster2
   KubeCluster2.scale
   Kubeluster2.get_logs
   KubeCluster2.close

.. autoclass:: KubeCluster2
   :members: