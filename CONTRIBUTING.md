Dask is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more. 

For general information on how to contribute see https://docs.dask.org/en/latest/develop.html.

## Project specific notes

For local development, it is often desirable to run the scheduler process on one’s local machine & the workers on minikube.
For more information on setting this up see https://kubernetes.dask.org/en/latest/testing.html.


### Dask operator - `go` client
We're also releasing a `go` client for the dask operator. In case you make changes to the custom resource definitions in `dask_kubernetes/operator/customresources/`, please also check whether this has an 
effect on any of the types in `dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1/types.go` and adapt them accordingly.

This being a Python repository, with a few (mostly auto-generated) `go` files, we've decided on an approach of linting an and code-generating `go` files within Docker. So in case you want to update the `go` files, no local `go` installation is required, but you will need to have `docker` ([docs](https://www.docker.com/)) installed.

#### Linting
In case you would like to lint the `go` files, run 
```bash
./dask_kubernetes/operator/go_client/hack/lint.sh
```
Linting will also happen automatically upon commit and on CI, using `pre-commit`. 

#### Code generation
In case you have changed the custom resource definitions, please port your changes to `dask_kubernetes/operator/go_client/pkg/apis/kubernetes.dask.org/v1/types.go` and then run

```bash
./dask_kubernetes/operator/go_client/hack/regenerate-code.sh
```
to regenerate all the files. Code-generation will also happen automatically upon commit and on CI, using `pre-commit`.

#### Versioning
`go` is very opinionated when it comes to versioning, especially regarding breaking changes. It's versioning system is designed around [Semantic Versioning](https://semver.org/) which is different from [Calendar Versioning](https://calver.org/) used throughout `dask`. In `go`, each major version is it's own package ([docs](https://go.dev/doc/modules/release-workflow#breaking)), which is why need to increase the version (i.e. the year from CalVer) in `./go.mod` as well as in `./dask_kubernetes/operator/go_client/hack/regenerate-code.sh` yearly for this work.
