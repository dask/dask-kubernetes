#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

setup(
    name='daskernetes',
    version='0.1.3',
    description='Native Kubernetes integration for Dask',
    url='https://github.com/dask/daskernetes',
    keywords='dask,kubernetes,distributed',
    license='BSD',
    packages=find_packages(),
    long_description=(open('README.rst').read() if exists('README.rst') else ''),
    zip_safe=False,
    install_requires=list(open('requirements.txt').read().strip().split('\n')),
)
