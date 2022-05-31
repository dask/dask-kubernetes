Installing
==========
.. currentmodule:: dask_kubernetes.experimental

.. warning::
    The Dask Operator for Kubernetes is experimental. So any `bug reports <https://github.com/dask/dask-kubernetes/issues>`_ are appreciated!

Installing with manifests
-------------------------

To install the the operator first we need to create the Dask custom resources:

.. code-block:: console

   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/deployment/manifests/daskcluster.yaml
   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/deployment/manifests/daskworkergroup.yaml
   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/deployment/manifests/daskjob.yaml

Then you should be able to list your Dask clusters via ``kubectl``.

.. code-block:: console

   $ kubectl get daskclusters
   No resources found in default namespace.

Next we need to install the operator itself. The operator is a small Python application that will watch the Kubernetes API for new Dask custom resources being created and add/remove pods/services/etc to create them.

.. code-block:: console

   $ kubectl apply -f https://raw.githubusercontent.com/dask/dask-kubernetes/main/dask_kubernetes/operator/deployment/manifests/operator.yaml

This will create the appropriate roles, service accounts and a deployment for the operator. We can check the operator pod is running:

.. code-block:: console

   $ kubectl get pods -A -l application=dask-kubernetes-operator
   NAMESPACE       NAME                                        READY   STATUS    RESTARTS   AGE
   dask-operator   dask-kubernetes-operator-775b8bbbd5-zdrf7   1/1     Running   0          74s


Installing with Helm
--------------------

Alternatively, the operator has a basic Helm chart which can be used to manage the installation of the operator.
The chart is published in the `Dask Helm repo <https://helm.dask.org>`_ repository, and can be installed via:

.. code-block:: console

    $ helm repo add dask https://helm.dask.org
    $ helm repo update
    $ helm install myrelease dask/dask-kubernetes-operator

This will install the custom resource definitions, service account, roles, and the operator deployment.

.. warning::
    Please note that `Helm does not support updating or deleting CRDs. <https://helm.sh/docs/chart_best_practices/custom_resource_definitions/#some-caveats-and-explanations>`_ If updates
    are made to the CRD templates in future releases (to support future k8s releases, for example) you may have to manually update the CRDs.

Kubeflow
--------

In order to use the Dask Operator with `Kubeflow <https://www.kubeflow.org/>`_ you need to perform some extra installation steps.

User permissions
^^^^^^^^^^^^^^^^

Kubeflow doesn't know anything about our Dask custom resource definitions so we need to update the ``kubeflow-kubernetes-edit`` cluster role. This role
allows users with cluster edit permissions to create pods, jobs and other resources and we need to add the Dask custom resources to that list.

.. code-block:: console

    $ kubectl patch clusterrole kubeflow-kubernetes-edit --patch '{"rules": [{"apiGroups": ["kubernetes.dask.org"],"resources": ["*"],"verbs": ["*"]}]}'
