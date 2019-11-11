import os
import sys
from remote_runner import SSHWorker, SyncSSHWorker


def ssh_worker_factory():
    worker = SSHWorker(host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
"""
    return worker


def sync_ssh_worker_factory():
    worker = SyncSSHWorker(sync_period=0.2, host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
"""
    return worker
