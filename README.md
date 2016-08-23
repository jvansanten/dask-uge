# dask-uge
Utility to launch a dask.distributed cluster on a UGE scheduler

## Problem addressed

The typical dask.distributed audience has a budget and access to whole
[virtual] machines via cloud providers. Academics, on the other hand,
may be restricted to a dedicated cluster where single slots can only be
captured for a limited time, and the nodes may be hidden behind layers of
inbound firewalls in interesting ways. `dask-uge` runs a `dask-scheduler`
on an execution node, forwards connections from the submission host to the
scheduler, and then keeps a pool of workers running, submitting new
ones as jobs are killed for exceeding their allotted time. In addition,
a short shell snippet can be included to initialize the environment on
the node (e.g. setting up a virtualenv) before the scheduler or worker
runs, in case the local UGE configuration makes it difficult to cleanly
forward the environment from the submission host.

# usage
`dask-uge.py --nworkers 10`

# known limitations
At the moment, `dask-uge` uses the default ports for `dask-scheduler`, and
the environment setup portion and job resource requests are hard-coded.
