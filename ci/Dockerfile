FROM daskdev/dask:latest

RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/local/src/dask_kubernetes
COPY . .

RUN make install
