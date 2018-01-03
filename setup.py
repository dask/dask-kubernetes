#!/usr/bin/env python

from os.path import exists
from setuptools import setup, find_packages

setup(
    name='daskernetes',
    version='0.0.1',
    description='Native kubernetes integration for dask distributed',
    url='https://github.com/yuvipanda/daskernetes',
    license='BSD',
    packages=find_packages(),
    long_description=(open('README.md').read() if exists('README.md') else ''),
    zip_safe=False,
    install_requires=[
        'kubernetes>=4.*'
    ]

)
