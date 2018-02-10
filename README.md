# daskernetes
Native Kubernetes integration for dask

**Note**: Extremely alpha quality right now!

## Setup

### testing

For local development, it is often desirable to run the scheduler
process on one's local machine & the workers on minikube.

1. Download, set up and start `minikube`
2. Make it possible for your host to be able to talk to the pods
   on minikube.

   On Linux,
   ```
   sudo ip route add 172.17.0.0/16 via $(minikube ip)
   ```

   On OS X,
   ```
   sudo route -n add -net 172.17.0.0/16 $(minikube ip)
   ```

   If you get an error message like:

   ```
   RTNETLINK answers: File exists
   ```

   it most likely means you have docker running on your host using
   the same IP range minikube is using. You can fix this by editing
   your `/etc/docker/daemon.json` to add:

   ```json
   {
       "bip": "172.19.1.1/16"
   }
   ```

   If some JSON already exists in that file, make sure to just add the
   `bip` key rather than replace it all. The final file needs to be
   valid JSON.

   Once edited, restart docker with `sudo systemctl restart docker`.
   It should come up using a different IP range, and you can run the
   `sudo ip route add` command again. Note that restarting docker
   will restart all your running containers by default.

3. Run tests

   ```bash
   py.test daskernetes --worker-image <worker-image>
   ```

   where:

      - `<worker-image>` is a docker image that has the same version of
        python and libraries installed as your host
      - `<cluster-name>` is a identifying name that you give to this
        specific cluster
      - `<namespace>` is the namespace you created in step 3


## History

This repository was originally inspired by a
[Dask+Kubernetes solution](https://github.com/met-office-lab/jade-dask/blob/master/kubernetes/adaptive.py)
within the
[Jade (Jupyter and Dask Environemt)](http://www.informaticslab.co.uk/projects/jade.html)
project out of the [UK Met office](https://www.metoffice.gov.uk/)
[Informatics Lab](http://www.informaticslab.co.uk/).
This Dask+Kubernetes solution was primarily developed by
[Jacob Tomlinson](https://github.com/jacobtomlinson) of the Informatics Lab
and [Matt Pryor](https://github.com/mkjpryor-stfc) of the
[STFC](http://www.stfc.ac.uk/) and funded by [NERC](http://www.nerc.ac.uk/).

![Met Office Logo](https://raw.githubusercontent.com/met-office-lab/blog/master/img/mo-logo.svg)

It was then adapted by [Yuvi Panda](http://words.yuvi.in/) at the
[UC Berkeley Institute for Data Science (BIDS)](https://bids.berkeley.edu/) and
[DSEP](http://data.berkeley.edu/) programs while using it with
[JupyterHub](https://jupyterhub.readthedocs.io/en/latest/) on the
[Pangeo project](https://pangeo-data.github.io/).
It was then brought under the Dask github organization where it lives today.
