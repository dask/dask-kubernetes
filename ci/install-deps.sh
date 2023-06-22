#!/bin/bash

set -e

curl -L https://istio.io/downloadIstio | sh -
mv istio-*/bin/istioctl /usr/local/bin/istioctl

pip install -e .
pip install -r requirements-test.txt
pip install git+https://github.com/dask/distributed@74a1bcd
pip install git+https://github.com/dask/dask@main
