IMAGE_NAME ?= dask-kubernetes
IMAGE_TAG ?= test

K8S_TEST_CONTEXT ?= kind-kind  # also tested with minikube
K8S_TEST_NAMESPACE ?= dask-kubernetes-test  # must have a serviceaccount, e.g. defined via `make k8s-set-up` 
COMMAND ?= test

# Pure python commands
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
	py.test dask_kubernetes/tests/test_async.py -vvv --namespace=${K8S_TEST_NAMESPACE}

# Docker commands
.PHONY: build docker-make

build:
	docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

docker-make:
	docker run -it ${IMAGE_NAME}:${IMAGE_TAG} ${COMMAND}

# Make test image available in-cluster.
# This is the only step that is not cluster-agnostic.
.PHONY: push

push-kind:
	kind load docker-image ${IMAGE_NAME}:${IMAGE_TAG}

push-minikube:  # this needs to be run before `build` to use locally built image in minikube
	eval $(shell minikube docker-env)

# Kubernetes commands
.PHONY: k8s-set-up k8s-test k8s-clean

k8s-set-up:
	kubectl --context=${K8S_TEST_CONTEXT} apply -f kubernetes/test-runner-setup.yaml

k8s-make:  # 
	kubectl --context=${K8S_TEST_CONTEXT} -n ${K8S_TEST_NAMESPACE} \
		run -i --tty --restart=Never \
		dask-kubernetes-test \
		--serviceaccount=test-runner \
		--image=${IMAGE_NAME}:${IMAGE_TAG} \
		--image-pull-policy=Never \
		--env="USER=tester" \
		--rm=true \
		${COMMAND}

k8s-clean:
	kubectl --context=${K8S_TEST_CONTEXT} -n ${K8S_TEST_NAMESPACE} delete all --all

# Botstrap a local cluster using https://github.com/kubernetes-sigs/kind
.PHONY: kind-bootstrap

kind-bootstrap:
	curl -Lo ./kind https://github.com/kubernetes-sigs/kind/releases/download/v0.6.1/kind-linux-amd64 && \
	chmod +x ./kind && \
	mv ./kind ~/.local/bin/ && \
	kind create cluster && \
	kind export kubeconfig
