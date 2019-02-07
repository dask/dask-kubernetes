#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

setup(
    name="dask-kubernetes",
    version="0.7.0",
    description="Native Kubernetes integration for Dask",
    url="https://github.com/dask/dask-kubernetes",
    keywords="dask,kubernetes,distributed",
    license="BSD",
    packages=find_packages(),
    include_package_data=True,
    long_description=(open("README.rst").read() if exists("README.rst") else ""),
    zip_safe=False,
    install_requires=list(open("requirements.txt").read().strip().split("\n")),
    python_requires=">=3.5",
)
