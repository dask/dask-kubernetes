
Contributing to Dask Kubernetes
===============================
Dask Kubernetes is a community maintained project. We welcome contributions in the form of bug reports, documentation, code, design proposals, and more. This page provides resources on how best to contribute.

Development environment
-----------------------

Download code
~~~~~~~~~~~~~

Make a fork of the main `Dask Kubernetes repository <https://github.com/Matt711/dask-kubernetes>`_ and
clone the fork::

   git clone git@github.com:<your-github-username>/dask-kubernetes.git
   cd dask-kubernetes

You should also add the remote upstream repository::

   git remote add upstream git@github.com:dask/dask-kubernetes.git

Contributions to Dask Kubernetes can then be made by submitting pull requests on GitHub.

Installing Dependencies
~~~~~~~~~~~~~~~~~~~~~~~

Install the required packages at the top level of your cloned repository::
    
    pip install -r requirements.txt

Running tests
'''''''''''''

Stability
'''''''''

Continuous integration testing
''''''''''''''''''''''''''''''

Documentation
'''''''''''''