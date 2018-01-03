# daskernetes
Native Kubernetes integration for dask

**Note**: Extremely alpha quality right now!

## Setup

### Telepresence

For local development, it is often desirable to run the scheduler
process on one's local machine & the workers on minikube. This is
most easily done with [Telepresence](https://www.telepresence.io/)

1. Download, set up and start `minikube`
2. Create a namespace to spawn your worker pods in.

   ```bash
   kubectl create dask-workers
   ```
   
3. Start the scheduler. This is currently out of process, but should
   be moved in-process soon.
   
   ```bash
   telepresence --expose 8736 --run python daskernetes/__init__.py --worker-image <worker-image> <cluster-name> <namespace>
   ```
   
   where:
   
      - `<worker-image>` is a docker image that has the same version of
        python and libraries installed as your host
      - `<cluster-name>` is a identifying name that you give to this 
        specific cluster
      - `<namespace>` is the namespace you created in step 3
      
5. In another terminal, start an ipython client and run the following code:

   ```python
   c = Client('0.0.0.0:8786')
   c.submit(lambda x:  x + 1, 10).result()
   ```

   This should dynamically spin up new workers & give you the output!
