#!/usr/bin/env python

from os.path import exists

from setuptools import find_packages, setup

import versioneer

setup(
    name="dask-kubernetes",
    cmdclass=versioneer.get_cmdclass(),
    version=versioneer.get_version(),
    description="Native Kubernetes integration for Dask",
    maintainer="Jacob Tomlinson",
    url="https://github.com/dask/dask-kubernetes",
    keywords="dask,kubernetes,distributed",
    license="BSD",
    packages=find_packages(),
    include_package_data=True,
    long_description=(open("README.rst").read() if exists("README.rst") else ""),
    zip_safe=False,
    install_requires=list(open("requirements.txt").read().strip().split("\n")),
    python_requires=">=3.9",
    entry_points="""
        [dask_cluster_discovery]
        helmcluster=dask_kubernetes.helm:discover
        kubecluster=dask_kubernetes.operator:discover
        [dask_operator_plugin]
        noop=dask_kubernetes.operator.controller.plugins.noop
        [dask_cli]
        kubernetes=dask_kubernetes.cli:main
      """,
)
