#!/bin/bash -e

CURRENT_DIR=$(dirname ${BASH_SOURCE})
REPO_DIR=$(realpath "$CURRENT_DIR/../../../..")

PROJECT_MODULE="github.com/dask/dask-kubernetes"
IMAGE_NAME="kubernetes-codegen:latest"

CUSTOM_RESOURCE_NAME="kubernetes.dask.org"
CUSTOM_RESOURCE_VERSION="v1"

echo "Building codegen Docker image..."
docker build -f "${CURRENT_DIR}/Dockerfile" \
             -t "${IMAGE_NAME}" \
             .

cmd="./generate-groups.sh all \
    "$PROJECT_MODULE/dask_kubernetes/operator/go_client/pkg/client" \
    "$PROJECT_MODULE/dask_kubernetes/operator/go_client/pkg/apis" \
    $CUSTOM_RESOURCE_NAME:$CUSTOM_RESOURCE_VERSION"


echo "Generating client codes..."
docker run --rm \
           -v "${REPO_DIR}:/go/src/${PROJECT_MODULE}" \
           "${IMAGE_NAME}" $cmd
