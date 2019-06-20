FROM daskdev/dask:latest

RUN conda uninstall -y --force dask-core distributed \
 && conda clean -afy
RUN pip install --no-deps --no-cache-dir \
    git+https://github.com/dask/dask \
    git+https://github.com/dask/distributed

ENTRYPOINT ["tini", "-g", "--", "/usr/bin/prepare.sh"]
