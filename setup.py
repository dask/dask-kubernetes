#!/usr/bin/env python

from os.path import exists
from setuptools import setup
import toolz

setup(name='daskernetes',
      version='0.0.1',
      description='',
      url='',
      license='BSD',
      packages=['daskernetes'],
      long_description=(open('README.md').read() if exists('README.md') else ''),
      zip_safe=False)
