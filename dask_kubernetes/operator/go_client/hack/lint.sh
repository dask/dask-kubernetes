#!/bin/bash -e

CURRENT_DIR=$(dirname ${BASH_SOURCE})
REPO_DIR=$(realpath "$CURRENT_DIR/../../../..")

docker run \
  --rm \
  -v "${REPO_DIR}":/app \
  -v ~/.cache/golangci-lint/v1.53.2:/root/.cache \
  -w /app \
  golangci/golangci-lint:v1.53.2 \
    golangci-lint \
    run -v \
    --enable goimports \
    --enable gofmt \
    --timeout 3m0s \
    ./...
