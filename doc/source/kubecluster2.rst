KubeCluster2
===========

:doc:`KubeCluster2` is for creating and managing a Dask cluster which using a Kubernetes Operator. With this cluster object you can conveniently create and manage a Dask :class:`dask.distributed.Client` object to the cluster and perform your work. Provided you have API access to Kubernetes and can run the ``kubectl`` command then connectivity to the Dask cluster is handled automatically for you via services or port forwarding.

Installing the Operator
----------

First you must install the Dask Operator and have access to a Kubernetes Cluster. To install the the operator, first you'll need to deploy the custom resources

.. code-block:: bash

   kubectl apply -f dask_kubernetes/operator/customresources/daskcluster.yaml
   kubectl apply -f dask_kubernetes/operator/customresources/daskworkergroup.yaml

Finally, you'll need to screate the operator

.. code-block:: bash
   kubectl apply -f dask_kubernetes/operator/deployment/manifest.yaml
   
Creating a Dask cluster via `kubectl`
---
To create a cluster in the default namespace, run the following
.. code-block:: bash
   kubectl apply -f dask_kubernetes/operator/tests/resources/simplecluster.yaml

You can create another yaml file following the `simplecluster.yaml` file to change
the default configuration (`namespace`, `replicas`, etc.) of your cluster.

You can scale the cluster
.. code-block:: bash
   kubectl scale --replicas=5 daskworkergroup simple-cluster-default-worker-group

Or add an additional worker group
.. code-block:: bash
   kubectl apply -f dask_kubernetes/operator/tests/resources/simpleworkergroup.yaml

Finally delete the cluster by running
.. code-block:: bash
   kubectl delete daskcluster simple-cluster

Creating a Dask cluster via the cluster manager `KubeCluster2`
---
To create a cluster in the default namespace, run the following
.. code-block:: python
   cluster = KubeCluster2(name = 'foo')

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

.. warning::
    `KubeCluster2` is experimental for now. So any bug reports are appreciated!