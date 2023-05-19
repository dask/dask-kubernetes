ARG BASE_IMAGE=ghcr.io/dask/dask:latest
FROM ${BASE_IMAGE}

# Install latest dev builds of Dask and Distributed
RUN pip install git+https://github.com/dask/distributed@main
RUN pip install git+https://github.com/dask/dask@main
