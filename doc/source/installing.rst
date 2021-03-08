Installing
==========

You can install dask-kubernetes with ``pip``, ``conda``, or by installing from source.

Pip
---

Pip can be used to install both dask-kubernetes and its dependencies.::

   pip install dask-kubernetes --upgrade # Install everything from last released version

Conda
-----

To install the latest version of dask-kubernetes from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.anaconda.com/downloads>`_::

    conda install dask-kubernetes -c conda-forge

Install from Source
-------------------

To install dask-kubernetes from source, clone the repository from `github
<https://github.com/dask/dask-kubernetes>`_::

    git clone https://github.com/dask/dask-kubernetes.git
    cd dask-kubernetes
    python setup.py install

or use ``pip`` locally if you want to install all dependencies as well::

    pip install -e .

You can also install directly from git main branch::

    pip install git+https://github.com/dask/dask-kubernetes
