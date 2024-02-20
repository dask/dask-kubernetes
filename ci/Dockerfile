ARG PYTHON=3.11

FROM ghcr.io/dask/dask:latest-py${PYTHON}

# Install latest dev builds of Dask and Distributed
RUN pip install git+https://github.com/dask/distributed@main
RUN pip install git+https://github.com/dask/dask@main
