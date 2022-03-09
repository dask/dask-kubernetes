# This images needs to be built from the top level of the project
# $ docker build -t ghcr.io/dask/dask-kubernetes-operator:latest -f dask_kubernetes/operator/deployment/Dockerfile .

FROM python:3.8

# Copy source
COPY . /src/dask_kubernetes
WORKDIR /src/dask_kubernetes

# Install dependencies
RUN pip install .

# Start operator
CMD kopf run -m dask_kubernetes.operator --verbose --all-namespaces
