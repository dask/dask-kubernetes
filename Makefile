IMAGE_NAME ?= dask-kubernetes
IMAGE_TAG ?= test

# kind-kind OR minikibe
K8S_TEST_CONTEXT ?= kind-kind
# must have a serviceaccount, e.g. defined via `make k8s-set-up`
K8S_TEST_NAMESPACE ?= dask-kubernetes-test
COMMAND ?= test
WORKER_IMAGE ?= ${IMAGE_NAME}:${IMAGE_TAG}
EXTRA_TEST_ARGS ?=

# Path to install binaries to
BIN_PATH ?= ~/.local/bin/

# Versions to install by default
OS ?= linux
ARCHITECTURE ?= amd64
KUBECTL_VERSION ?= v1.17.0
KIND_VERSION ?= v0.6.1
MINIKUBE_VERSION ?= v0.25.2

# Pure python commands
.PHONY: install format lint test

install:
	pip install -e .
	pip install -r requirements-dev.txt

format:
	black dask_kubernetes setup.py

lint:
	flake8 dask_kubernetes
	black --check dask_kubernetes setup.py

test:
	py.test dask_kubernetes -vvv \
		--namespace=${K8S_TEST_NAMESPACE} --worker-image=${WORKER_IMAGE} ${EXTRA_TEST_ARGS}

# Docker commands
.PHONY: build docker-make

build:
	docker build -t ${IMAGE_NAME}:${IMAGE_TAG} -f ci/Dockerfile .

docker-make:
	docker run -it --entrypoint make ${IMAGE_NAME}:${IMAGE_TAG} ${COMMAND}

# Make test image available in-cluster.
# This is the only step that is not cluster-agnostic.
.PHONY: push-kind

push-kind:
	kind load docker-image ${IMAGE_NAME}:${IMAGE_TAG}

# Kubernetes commands
.PHONY: k8s-deploy k8s-test k8s-clean

k8s-deploy:
	kubectl --context=${K8S_TEST_CONTEXT} apply -f ci/test-runner-setup.yaml

k8s-make:  # having to set USER is actually a bug
	kubectl --context=${K8S_TEST_CONTEXT} -n ${K8S_TEST_NAMESPACE} \
		run -i --tty --restart=Never \
		dask-kubernetes-test \
		--serviceaccount=test-runner \
		--image=${IMAGE_NAME}:${IMAGE_TAG} \
		--image-pull-policy=Never \
		--env="USER=tester" \
		--env="K8S_TEST_NAMESPACE=${K8S_TEST_NAMESPACE}" \
		--env="EXTRA_TEST_ARGS=--in-cluster" \
		--rm=true \
		--command -- make ${COMMAND}

k8s-clean:
	kubectl --context=${K8S_TEST_CONTEXT} -n ${K8S_TEST_NAMESPACE} delete all --all

# Install kubectl and local cluster
.PHONY: kubectl-bootstrap kind-bootstrap minikube-bootstrap

kubectl-bootstrap:
	curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/${OS}/${ARCHITECTURE}/kubectl && \
	chmod +x kubectl && \
	mv kubectl ${BIN_PATH}

kind-bootstrap:  # https://github.com/kubernetes-sigs/kind
	curl -Lo ./kind https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-${OS}-${ARCHITECTURE} && \
	chmod +x ./kind && \
	mv kind ${BIN_PATH}

kind-start:
	kind create cluster && \
	kind export kubeconfig

minikube-bootstrap:
	curl -Lo minikube https://github.com/kubernetes/minikube/releases/download/${MINIKUBE_VERSION}/minikube-${OS}-${ARCHITECTURE} && \
	chmod +x minikube && \
	mv minikube ${BIN_PATH}

minikube-start:  # this needs to be run before `build` to use locally built image in minikube
	minikube start --vm-driver=none --extra-config=kubelet.MaxPods=20 && \
	eval $(shell minikube docker-env)
