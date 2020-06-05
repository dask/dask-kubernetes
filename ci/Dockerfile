FROM daskdev/dask:latest

WORKDIR /usr/local/src
COPY . .

RUN pip install -e . && pip install -r requirements-dev.txt

RUN apt-get update && apt-get install curl -y && \
    curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash