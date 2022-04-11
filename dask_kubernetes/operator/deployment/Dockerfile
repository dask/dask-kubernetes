# This images needs to be built from the top level of the project
# $ docker build -t ghcr.io/dask/dask-kubernetes-operator:latest -f dask_kubernetes/operator/deployment/Dockerfile .

FROM python:3.8

# Copy source
COPY . /src/dask_kubernetes
WORKDIR /src/dask_kubernetes

# Install dependencies
RUN pip install .

# We don't use TARGETARCH so as to support non-buildkit builds
RUN MAGICARCH=$(dpkg --print-architecture) && \
    curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/${MAGICARCH}/kubectl" && \
    mkdir -p /usr/local/bin && \
    mv ./kubectl /usr/local/bin/kubectl && \
    chmod +x /usr/local/bin/kubectl && \
    kubectl version --client

# Start operator
CMD kopf run -m dask_kubernetes.operator --verbose --all-namespaces
