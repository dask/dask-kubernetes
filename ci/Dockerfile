FROM daskdev/dask:latest

WORKDIR /usr/local/src
COPY . .

# Install python dependencies
RUN pip install -e . && pip install -r requirements-dev.txt

# Install helm
RUN apt-get update && apt-get install curl make -y && \
    curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash

# Install the Dask chart repo using Helm
RUN helm repo add dask https://helm.dask.org && helm repo update
