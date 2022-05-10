#!/bin/bash

pip install -e .
pip install -r requirements-test.txt
pip install git+https://github.com/dask/distributed@main
pip install git+https://github.com/dask/dask@main
