# This images needs to be built from the top level of the project
# $ docker build -t ghcr.io/dask/dask-kubernetes-operator:latest -f dask_kubernetes/operator/deployment/Dockerfile .

FROM python:3.10 as builder

# Copy source
COPY . /src/dask_kubernetes
WORKDIR /src/dask_kubernetes

# Build dask-kubernetes distribution
RUN pip install hatch && hatch build

FROM python:3.10

# Copy wheel
COPY --from=builder /src/dask_kubernetes/dist /src/dask_kubernetes/dist
WORKDIR /src/dask_kubernetes

# Install from wheel
RUN pip install /src/dask_kubernetes/dist/dask_kubernetes*.whl

# Add non-root user
RUN useradd -s /bin/bash dask

# Run container as non-root
USER 1000

# Start operator
ENTRYPOINT ["/usr/local/bin/kopf", "run", "-m", "dask_kubernetes.operator.controller"]
CMD ["--liveness=http://0.0.0.0:8080/healthz", "--verbose", "--all-namespaces"]
