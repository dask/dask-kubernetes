#!/bin/bash

FULL="dask_kubernetes/operator/deployment/manifests/full.yaml"
echo > $FULL

for f in dask_kubernetes/operator/deployment/manifests/dask*.yaml; do
    cat $f >> $FULL
    echo "---" >> $FULL
done
