#!/bin/bash -e

CURRENT_DIR=$(dirname ${BASH_SOURCE})
REPO_DIR=$(realpath "$CURRENT_DIR/../../../..")
MAJOR_VERSION="v2024"

PROJECT_MODULE="github.com/dask/dask-kubernetes/${MAJOR_VERSION}"
IMAGE_NAME="kubernetes-codegen:latest"

CUSTOM_RESOURCE_NAME="kubernetes.dask.org"
CUSTOM_RESOURCE_VERSION="v1"

echo "Building codegen Docker image..."
docker build -f "${CURRENT_DIR}/Dockerfile" \
             -t "${IMAGE_NAME}" \
             .

echo "Generating client codes..."
docker run --rm \
           -v "${REPO_DIR}:/go/src/${PROJECT_MODULE}" \
           -w "/go/src/${PROJECT_MODULE}" \
           "${IMAGE_NAME}" \
           /go/src/k8s.io/code-generator/generate-groups.sh \
            all \
            $PROJECT_MODULE/dask_kubernetes/operator/go_client/pkg/client \
            $PROJECT_MODULE/dask_kubernetes/operator/go_client/pkg/apis \
            $CUSTOM_RESOURCE_NAME:$CUSTOM_RESOURCE_VERSION \
            -h /root/boilerplate.txt
