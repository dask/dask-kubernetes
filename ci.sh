#!/bin/bash
# Run the tests, capture the error condition,
# and on error print some docker & kube info.
set -x
set +e
pytest -s -v -l --tb=long --full-trace
code=$?
if [ "$code" == "0" ]; then
  exit $code
fi
docker ps
export KUBECONFIG=.pytest-kind/pytest-kind/kubeconfig
kubcetl get nodes
exit $code
