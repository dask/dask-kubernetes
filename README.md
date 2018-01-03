# daskernetes
Native Kubernetes integration for dask

**Note**: Extremely alpha quality right now!

## Setup

### Minikube setup

For local development, it is often desirable to run the scheduler
process on one's local machine & the workers on minikube.

1. Download, set up and start `minikube`
2. Make it possible for your host to be able to talk to the pods
   on minikube. Note that this might do interesting things to
   docker running on your host, so be careful!
   
   On Linux,
   ```
   sudo ip route add 172.17.0.0/16 via $(minikube ip)
   ```
   
   On OS X,
   ```
   sudo route -n add -net 172.17.0.0/16 $(minikube ip)
   ```
3. Create a namespace to spawn your worker pods in.

   ```bash
   kubectl create dask-workers
   ```
   
4. Start the scheduler. This is currently out of process, but should
   be moved in-process soon.
   
   ```bash
   python daskernetes/__init__.py --worker-image <worker-image> <cluster-name> <namespace>
   ```
   
   where:
   
      - `<worker-image>` is a docker image that has the same version of
        python and libraries installed as your host
      - `<cluster-name>` is a identifying name that you give to this 
        specific cluster
      - `<namespace>` is the namespace you created in step 3
      
5. In another terminal, start an ipython client and run the following code:

   ```python
   c = Client('127.0.0.1:8786')
   c.submit(lambda x:  x + 1, 10).result()
   ```

   This should dynamically spin up new workers & give you the output!
