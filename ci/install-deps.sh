#!/bin/bash

set -e

curl -L https://istio.io/downloadIstio | sh -
mv istio-*/bin/istioctl /usr/local/bin/istioctl

pip install -e .
pip install -r requirements-test.txt
pip install git+https://github.com/dask/distributed@4a0c48956ff47b900c7e285a843f722d8e13aa03
pip install git+https://github.com/dask/dask@main
