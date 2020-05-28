FROM daskdev/dask:latest

WORKDIR /usr/local/src
COPY . .

RUN pip install -e . && pip install -r requirements-dev.txt
