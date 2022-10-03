Extending (advanced)
====================
.. currentmodule:: dask_kubernetes.operator.kubecluster

You can extend the functionality of the Dask Operator controller by writing plugins.
You may wish to do this if you want the operator to create other resources like Istio ``VirtualSerivce``, ``Gateway`` and ``Certificate`` resources.
Extra resources like this may end up being a common requirement, but given the endless possibilities of k8s cluster setups it's hard to make this configurable.

To help cluster administrators ensure the Dask Operator does exactly what they need we support extending the controller via plugins.

Controller Design Overview
--------------------------

The Dask Operator's controller is built using `kopf <https://kopf.readthedocs.io/en/stable/>`_ which allows you to write custom handler functions in Python for any Kubernetes event.
The Dask Operator has a selection of :doc:`Custom Resources <operator_resources>` and the controller handles create/update/delete events for these resources.
For example whenever a ``DaskCluster`` resource is created the controller sets the ``status.phase`` attribute to ``Created``.

.. code-block:: python

   @kopf.on.create("daskcluster")
   async def daskcluster_create(name, namespace, logger, patch, **kwargs):
      """When DaskCluster resource is created set the status.phase.

      This allows us to track that the operator is running.
      """
      logger.info(f"DaskCluster {name} created in {namespace}.")
      patch.status["phase"] = "Created"

Then there is another handler that watches for ``DaskCluster`` resources that have been put into this ``Created`` phase.
This handler creates the ``Pod``, ``Service`` and ``DaskWorkerGroup`` subresources of the cluster and then puts it into a ``Running`` phase.

.. code-block:: python

   @kopf.on.field("daskcluster", field="status.phase", new="Created")
   async def daskcluster_create_components(spec, name, namespace, logger, patch, **kwargs):
      """When the DaskCluster status.phase goes into Pending create the cluster components."""
      async with kubernetes.client.api_client.ApiClient() as api_client:
         api = kubernetes.client.CoreV1Api(api_client)
         custom_api = kubernetes.client.CustomObjectsApi(api_client)

         # Create scheduler Pod
         data = build_scheduler_pod_spec(...)
         kopf.adopt(data)
         await api.create_namespaced_pod(namespace=namespace, body=data)

         # Create scheduler Service
         data = build_scheduler_service_spec(...)
         kopf.adopt(data)
         await api.create_namespaced_service(namespace=namespace, body=data)

         # Create DaskWorkerGroup
         data = build_worker_group_spec(...)
         kopf.adopt(data)
         await custom_api.create_namespaced_custom_object(group="kubernetes.dask.org", version="v1", plural="daskworkergroups", namespace=namespace, body=data)

      # Set DaskCluster to Running phase
      patch.status["phase"] = "Running"

Then when the ``DaskWorkerGroup`` resource is created that triggers the worker creation event handler which creates more ``Pod`` resources.
In turn the creation of ``Pod`` and ``Service`` resources will be triggering internal event handlers in Kubernetes which will create containers, set iptable rules, etc.

This model of writing small handlers that are triggered by events in Kubernetes allows you to create powerful tools with simple building blocks.

Writing your own handlers
-------------------------

To avoid users having to write their own controllers the Dask Operator controller supports loading additional handlers from other packages via ``entry_points``.

Custom handlers must be `packaged as a Python module <https://packaging.python.org/en/latest/tutorials/packaging-projects/>`_ and be importable.

For example let's say you have a minimal Python package with the following structure:

.. code-block::

   my_controller_plugin/
   ├── pyproject.toml
   └── my_controller_plugin/
       ├── __init__.py
       └── plugin.py

If you wanted to write a custom handler that would be triggered when the scheduler ``Service`` is created then ``plugin.py`` would look like this:

.. code-block:: python

   import kopf

   @kopf.on.create("service", labels={"dask.org/component": "scheduler"})
   async def handle_scheduler_service_create(meta, new, namespace, logger, **kwargs):
      # Do something here
      # See https://kopf.readthedocs.io/en/stable/handlers for documentation on what is possible here

Then you need to ensure that your ``pyproject.toml`` registers the plugin as a ``dask_operator_plugin``.

.. code-block:: toml

   ...

   [option.entry_points]
   dask_operator_plugin =
      my_controller_plugin = my_controller_plugin.plugin

Then you can package this up and push it to your preferred Python package repository.

Installing your plugin
----------------------

When the Dask Operator controller starts up it checks for any plugins registered via the ``dask_operator_plugin`` entry point and loads those too.
This means that installing your plugin is as simple as ensuring your plugin package is installed in the controller container image.

The controller uses the ``ghcr.io/dask/dask-kubernetes-operator:latest`` container image by default so your custom container ``Dockerfile`` would look something like this:

.. code-block:: Dockerfile

   FROM ghcr.io/dask/dask-kubernetes-operator:latest

   RUN pip install my-controller-plugin

Then when you :doc:`install the controller deployment <operator_installation>` either via the manifest or with helm you would specify your custom container image instead.

.. code-block:: bash

   helm install --set image.name=my_controller_image myrelease dask/dask-kubernetes-operator
