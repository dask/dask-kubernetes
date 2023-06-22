#!/bin/bash

set -e

curl -L https://istio.io/downloadIstio | sh -
mv istio-*/bin/istioctl /usr/local/bin/istioctl

pip install -e .
pip install -r requirements-test.txt
pip install git+https://github.com/dask/distributed@e7cb0d80fad01c34ed0418057a062dfb0971e2c8
pip install git+https://github.com/dask/dask@main
