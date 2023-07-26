# This images needs to be built from the top level of the project
# $ docker build -t ghcr.io/dask/dask-kubernetes-operator:latest -f dask_kubernetes/operator/deployment/Dockerfile .

FROM python:3.10

# Copy source
COPY . /src/dask_kubernetes
WORKDIR /src/dask_kubernetes

# Install dependencies
RUN pip install .

# Add non-root user
RUN useradd -s /bin/bash dask

# Run container as non-root
USER 1000

# Start operator
ENTRYPOINT ["/usr/local/bin/kopf", "run", "-m", "dask_kubernetes.operator.controller"]
CMD ["--liveness=http://0.0.0.0:8080/healthz", "--verbose", "--all-namespaces"]
