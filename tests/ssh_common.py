import os
import sys
from remote_runner import SSHWorker


def ssh_worker_factory():
    worker = SSHWorker(host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
"""
    return worker
