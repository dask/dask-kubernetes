Daskernetes
===========

Daskernetes deploys Dask workers on Kubernetes clusters

Install
-------

::

   pip install daskernetes

Quickstart
----------

.. code-block:: yaml

      # worker-spec.yml

      kind: Pod
      metadata:
        labels:
          foo: bar
      spec:
        restart_policy: Never
        containers:
        - image: daskdev/dask:latest
          args: [dask-worker, --nthreads, '2', --no-bokeh, --memory-limit, 8GB, --death-timeout, '60']
          env:
            - name: EXTRA_PIP_PACKAGES
              value: fastparquet git+https://github.com/dask/distributed
          resources:
            limits:
              cpu: "3"
              memory: 8G
            requests:
              cpu: "2"
              memory: 8G

.. code-block:: python

   from daskernetes import KubeCluster

   cluster = KubeCluster.from_yaml('worker-spec.yml')
   cluster.scale_up(10)  # specify number of nodes explicitly

   cluster.adapt()  # or dynamically scale based on current workload

API Documentation
-----------------

.. currentmodule:: daskernetes

.. autosummary::
   KubeCluster
   KubeCluster.adapt
   KubeCluster.from_dict
   KubeCluster.from_yaml
   KubeCluster.logs
   KubeCluster.pods
   KubeCluster.scale_up

.. autoclass:: KubeCluster
   :members:
