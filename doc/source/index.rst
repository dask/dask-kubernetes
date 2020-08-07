Dask Kubernetes
===============

Dask Kubernetes provides cluster managers for Kubernetes.

``KubeCluster`` deploys Dask workers on Kubernetes clusters using native
Kubernetes APIs.  It is designed to dynamically launch short-lived deployments
of workers during the lifetime of a Python process.

Currently, it is designed to be run from a pod on a Kubernetes cluster that
has permissions to launch other pods. However, it can also work with a remote
Kubernetes cluster (configured via a kubeconfig file), as long as it is possible
to interact with the Kubernetes API and access services on the cluster.

``HelmCluster`` is for managing an existing Dask cluster which has been deployed using
`Helm <https://helm.sh>`_. You must have already installed the `Dask Helm chart <https://helm.dask.org/>`_
and have the cluster running. You can then use ``HelmCluster`` to manage scaling and retrieve logs.

See https://docs.dask.org/en/latest/setup/kubernetes.html for more.

Quickstart
----------

- :ref:`kubecluster`
- :ref:`helmcluster`


.. toctree::
   :maxdepth: 1
   :hidden:

   installing
   kubecluster
   helmcluster
   api
   history
   testing
