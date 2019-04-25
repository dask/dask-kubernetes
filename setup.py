#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

import versioneer

setup(
    name='dask-kubernetes',
    cmdclass=versioneer.get_cmdclass(),
    version=versioneer.get_version(),
    description='Native Kubernetes integration for Dask',
    url='https://github.com/dask/dask-kubernetes',
    keywords='dask,kubernetes,distributed',
    license='BSD',
    packages=find_packages(),
    include_package_data=True,
    long_description=(open('README.rst').read() if exists('README.rst') else ''),
    zip_safe=False,
    install_requires=list(open('requirements.txt').read().strip().split('\n')),
    python_requires='>=3.5',
)
