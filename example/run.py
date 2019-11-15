from pathlib import Path
from remote_runner import Pool, SyncSSHWorker, SSHWorker, LocalWorker, Task

source_venv = "source ~/venv/bin/activate\n"

remote_tmp_root = Path("~/remote_tmp_root")


# customize LocalWorker class for our needs
class LocalCudaWorker(LocalWorker):
    def __init__(self, device: int):
        self.device = device

    def run(self, task):
        import os
        os.environ["CUDA_VISIBLE_DEVICES"] = str(self.device)
        os.environ["WORKER_ID"] = "LocalCudaWorker-0"
        super().run(task)


workers = [
    # worker which sync periodically and at the end of task
    SyncSSHWorker(
        host='localhost',  # alias from .ssh/config
        sync_period=600,  # run rsync every 5 min
        remote_root=remote_tmp_root,  # where to place task folders on remote
        remote_user_rc="export CUDA_VISIBLE_DEVICES=0\n"  # enforce use of first GPU,
                       "export WORKER_ID=SyncSSHWorker-0; \n"  # mark task's worker
                       + source_venv
    ),
    # worker which sync only at the end of task
    SSHWorker(
        host='localhost',  # alias from .ssh/config
        remote_root=remote_tmp_root,  # where to place task folders on remote
        remote_user_rc="export CUDA_VISIBLE_DEVICES=1\n"  # enforce use of second GPU
                       "export WORKER_ID=SSHWorker-0;\n"  # mark task's worker
                       + source_venv
    ),
    # simple worker which runs locally, no folder copying, no ssh. Runs as multiprocessing.Process
    LocalCudaWorker(
        device=3  # use third GPU
    )
]

tasks = [
    Task.load(task_file)
    for task_file in Path("./").glob("*/state.dill")  # find all task in subdirectories
]

Pool(workers).run(tasks)
