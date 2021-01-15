Testing
=======

During local development, we may want to test the code in three different scenarios, largely
corresponding to real-world deployment scenarios:

1. Run code locally (possibly inside a `virtual environment <https://docs.python.org/3/tutorial/venv.html>`_)
2. Dockerize code and run it inside a `docker container <https://www.docker.com/resources/what-container>`_
3. Run code inside a `Kubernetes Pod <https://kubernetes.io/docs/concepts/workloads/pods/pod/>`_

Running *integration tests* with either of these options requires access to a Kubernetes cluster
to spawn worker pods.

.. note:: In CI, we perform static checks in a container (option 2 above), and run tests in a Pod (option 3 above)

Set up local development environment
------------------------------------
1. Make sure you have ``make`` installed
2. Run ``make install`` to install dependencies in your current python environment

You can now run ``make lint``, ``make test`` etc. - however, many tests will require access
to a kubernetes cluster, as described below.

Set up a local Kubernetes cluster
---------------------------------
We can set up a kubernetes cluster on the local machine using either
`minikube <https://minikube.sigs.k8s.io/>`_ or `kind <https://kind.sigs.k8s.io/>`_

``minikube``
^^^^^^^^^^^^
1. Install ``minikube`` by following `online instructions <https://kubernetes.io/docs/tasks/tools/install-minikube/>`__, or use::

      make kubectl-bootstrap
2. Start ``minikube`` with::

      minikube start
3. (recommended) to run containerized tests in a pod, make sure you have `installed docker <https://docs.docker.com/install/>`__ and run ``eval $(shell minikube docker-env)`` before building the Docker image
4. (optional) if instead you want to run tests locally, you don't need ``docker``, but you will need to make it possible for your host to be able to talk to the pods on minikube - see section below.
5. Create a namespace and role bindings for testing::

      make k8s-deploy K8S_TEST_CONTEXT=minikube

(Optional) Configure network access to the ``minikube`` pods
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
.. note::  This step may vary depending on your network configuration, and it may be easier to run containerized tests in-cluster instead

First lookup the IP range used by the docker instance in minikube:

- Use the docker daemon inside minikube::

    eval $(minikube -p minikube docker-env)

- Now lookup the ip subnet used by minikube

    docker network inspect -f '{{(index .IPAM.Config 0).Subnet}}' bridge

Now setup a route to reach pods running in minikube:

- On Linux::

   sudo ip route add 172.17.0.0/16 via $(minikube ip)

- On OS X::

   sudo route -n add -net 172.17.0.0/16 $(minikube ip)

If you get an error message like the following::

   RTNETLINK answers: File exists

it most likely means you have docker running on your host using the same
IP range minikube is using. You can fix this by editing your
``/etc/docker/daemon.json`` file to add the following:

.. code-block:: json

   {
      "bip": "172.19.1.1/16"
   }

If some JSON already exists in that file, make sure to just add the
``bip`` key rather than replace it all. The final file needs to be valid
JSON.

Once edited, restart docker with ``sudo systemctl restart docker``. It
should come up using a different IP range, and you can run the
``sudo ip route add`` command again. Note that restarting docker will
restart all your running containers by default.

``kind``
^^^^^^^^
0. Make sure you have `installed docker <https://docs.docker.com/install/>`__
1. Install ``kind`` by following `online instructions <https://kind.sigs.k8s.io/docs/user/quick-start#installation>`__, or use::

      make kind-bootstrap
2. Start ``kind``::

      make kind-start
3. Create a namespace and role bindings for testing::

      make k8s-deploy

4. Remember that local images will need to be pushed to ``kind`` nodes with ``make push-kind``

Build a docker image for Testing
--------------------------------
1. Ensure you have `installed docker <https://docs.docker.com/install/>`__
2. Build docker image::

      make build

3. (if using ``kind``) push image to cluster nodes::

      make push-kind

Run tests locally
-----------------
.. note: Running tests locally is not possible if using kind

1. Check code for formatting errors::

      make lint
2. (Optional) run ``kubectl config use-context <context>``, where ``context`` is either ``kind-kind`` or ``minikube``
3. Run tests with ``make test``

Run tests in a container
------------------------
Any make command, e.g. ``make lint``, can be executed in the pre-built container using::

   make docker-make COMMAND=lint

.. note::  By default, local code is mounted in the docker container, so you don't need to rebuild the image to see local changes to your code or tests.
.. note:: Tests requiring cluster access will not run without further setup, run them in a Pod insted - see below

Run tests in a pod
------------------
Similar to running tests in a docker container, simply run::

   make k8s-make COMMAND=test
