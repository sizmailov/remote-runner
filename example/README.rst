Here is an example of custom setup.

Generally it's good idea to split setup in two stages:

1. Define business logic of custom task in `init.py` by

2. Define workers which will execute specified tasks in `run.py` and start execution

Once scripts written your should be able to create and run tasks:

.. code-block:: bash

    python init.py  # create
    python run.py   # run


Possible output:

.. code-block::

    00/stdout:WORKER_ID = SSHWorker-0
    01/stdout:WORKER_ID = SyncSSHWorker-0
    02/stdout:WORKER_ID = LocalCudaWorker-0
    03/stdout:WORKER_ID = SyncSSHWorker-0
    04/stdout:WORKER_ID = LocalCudaWorker-0
    05/stdout:WORKER_ID = LocalCudaWorker-0
    06/stdout:WORKER_ID = LocalCudaWorker-0
    07/stdout:WORKER_ID = LocalCudaWorker-0
    08/stdout:WORKER_ID = LocalCudaWorker-0
    09/stdout:WORKER_ID = SSHWorker-0





**Note 1**: module-level import statements in `init.py` will not take effect on remote machine, therefore
all necessary imports are should be placed in `run()` method body

**Note 2**: `super()` calls in Task-derived classes will result into deserializing problems (see https://github.com/uqfoundation/dill/issues/300 )
To avoid it, name base class(es) explicitly





