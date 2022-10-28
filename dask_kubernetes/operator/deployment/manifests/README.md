# Dask Operator Static Manifests

Looking for the static manifests for installing the Dask Operator? Sadly we have removed them.

To ensure our users have a good experience installing the operator we need to be confident that things work out of the box and [we couldn't be confident that
applying a manifest from a `main` branch of a git repo would give that experience](https://github.com/dask/dask-kubernetes/issues/508).

We recommend all users use the [Helm Chart](https://kubernetes.dask.org/en/latest/operator_installation.html#installing-with-helm) for installation.

If you prefer not to use `helm` and instead apply static manifests with `kubectl` then you can quickly export them from the chart with the following command.

```console
$ helm template --include-crds --repo https://helm.dask.org release dask-kubernetes-operator | kubectl apply -f -
```
