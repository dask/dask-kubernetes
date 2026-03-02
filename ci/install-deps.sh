#!/bin/bash

set -e

curl -L https://istio.io/downloadIstio | sh -
mv istio-*/bin/istioctl /usr/local/bin/istioctl

pip install -e .
pip install -r requirements-test.txt
pip install git+https://github.com/dask/distributed@main
pip install git+https://github.com/dask/dask@main
# Re-pin k8s-crd-resolver compatible versions after other installs
# may upgrade them:
# - setuptools 82 removed pkg_resources, needed by openapi-spec-validator<0.5.0
# - openapi-spec-validator<0.5.0 needs jsonschema<4.18 for _legacy_validators
pip install "setuptools<81" "jsonschema==4.17.3" "openapi-spec-validator<0.5.0"
