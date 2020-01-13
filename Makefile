KUBE_CONTEXT ?= kind-kind
IMAGE_NAME ?= dask-kubernetes
IMAGE_TAG ?= test

.PHONY: install format lint test

install:
	pip install -e .
	pip install -r requirements-dev.txt

format:
	black dask_kubernetes setup.py

lint:
	flake8 dask-kubernetes
	black --check dask_kubernetes setup.py

test:
	py.test dask_kubernetes/tests/test_async.py --context=${KUBE_CONTEXT} -vvv

build:
	docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .
