from .utility import (ChangeDirectory, ChangeToTemporaryDirectory)
import time
from typing import List
import queue
import hashlib
import threading
import subprocess
from pathlib import Path
import paramiko
import os
import shlex
import dill


class Task:

    def __init__(self, wd: Path):
        assert wd.is_dir()
        self.wd = wd

    def save(self, filename: Path):
        tmp = Path(f"{filename}.bak")
        with tmp.open('wb') as out:
            if out:
                dill.dump(self, out)
                out.close()
        tmp.rename(filename)

    @staticmethod
    def load(filename: Path) -> 'Task':
        with filename.open("rb") as f:
            if f:
                result = dill.load(f)
            else:
                raise OSError("Could not open '%s' for reading." % filename)
        return result

    def run(self):
        raise NotImplementedError()


class Worker:
    def run(self, task: Task):
        raise NotImplementedError()

    def stage_in(self, task: Task):
        raise NotImplementedError()

    def stage_out(self, task: Task):
        raise NotImplementedError()


class SSHWorker(Worker):
    ssh_config_file = "~/.ssh/config"
    rsync_to_remote_args = ['-a']
    rsync_to_local_args = ['-a']

    def __init__(self, host: str, remote_root: Path = None):
        self.host = host
        if remote_root is not None:
            remote_root = Path("~/")
        self.remote_root: Path = remote_root
        self._ssh = None

    def remote_wd(self, local_wd: Path):
        md5sum = hashlib.md5().update(str(local_wd.absolute()).encode('utf-8'))
        return self.remote_root / md5sum

    def rsync(self, args, src, dst):
        rsync_cmd = ["rsync"] + args + [src, dst]
        subprocess.call(rsync_cmd)  # todo: add timeout

    def remote_call(self, cmd: List[str], stdin=None, sleep_time=0.05):
        channel = self.ssh.get_transport().open_session()
        cmd_line = ' '.join(shlex.quote(x) for x in cmd)  # shelx.join(cmd)
        channel.exec_command(cmd_line)
        if stdin is not None:
            inp = channel.makefile('wb', -1)
            inp.write(stdin)
            inp.flush()
            inp.close()
            channel.shutdown_write()

        out = b''
        err = b''

        while not channel.exit_status_ready():  # monitoring process
            while channel.recv_ready():
                out += channel.recv(80)
            while channel.recv_stderr_ready():
                err += channel.recv_stderr(80)
            time.sleep(sleep_time)
            if sleep_time < 1:
                sleep_time *= 2

        while channel.recv_ready():
            out += channel.recv(80)
        while channel.recv_stderr_ready():
            err += channel.recv_stderr(80)

        return channel.recv_exit_status(), out, err

    def stage_in(self, task: Task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        self.remote_call(['mkdir', '-p', remote])
        self.rsync(self.rsync_to_remote_args, f"{local}/", f"{self.host}:{remote}/")

    def stage_out(self, task: Task):
        remote = self.copy_to_local(task)
        self.remote_call(['rm', '-rf', remote])

    def copy_to_local(self, task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        self.rsync(self.rsync_to_remote_args, f"{self.host}:{remote}/", f"{local}/", )
        return remote

    def run(self, task: Task):
        self.stage_in(task)
        try:
            self.run_remotely(task)
        finally:
            self.stage_out(task)

    def run_remotely(self, task: Task):
        raise NotImplementedError()

    @property
    def ssh(self):
        if self._ssh is None:  # todo: check ssh connection health
            self._ssh = self._new_ssh_client()
        return self._ssh

    def _get_host_config(self):
        ssh_config = paramiko.SSHConfig()
        user_config_file = os.path.expanduser(self.ssh_config_file)

        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config.parse(f)

        cfg = {'hostname': self.host}
        user_config = ssh_config.lookup(cfg['hostname'])
        if 'hostname' in user_config:
            cfg['hostname'] = user_config['hostname']
        if 'user' in user_config:
            cfg['username'] = user_config['user']
        for k in ('port',):
            if k in user_config:
                cfg[k] = int(user_config[k])
        if 'proxycommand' in user_config:
            cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])

        return cfg

    def _new_ssh_client(self):
        client = paramiko.SSHClient()
        client._policy = paramiko.WarningPolicy()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(**self._get_host_config())
        return client


class LocalWorker(Worker):
    def run(self, task: Task):
        task.run()

    def stage_in(self, task: Task):
        pass

    def stage_out(self, task: Task):
        pass


class Runner(threading.Thread):
    def __init__(self, worker: Worker, tasks: 'queue.Queue[Task]'):
        super().__init__()
        self.worker = worker
        self.tasks = tasks

    def run(self):
        try:
            while True:
                task = self.tasks.get(block=False)
                with ChangeDirectory(task.wd):
                    self.worker.run(task)  # todo: handle exceptions
                self.tasks.task_done()
        except queue.Empty:
            pass


class Pool:
    def __init__(self, workers: List[Worker]):
        self.workers = workers

    def run(self, tasks: List[Task]):
        tasks_queue = queue.Queue(maxsize=len(tasks))
        runners = [Runner(worker=worker, tasks=tasks_queue) for worker in self.workers]

        for task in tasks:
            tasks_queue.put(task)

        for runner in runners:
            runner.start()

        for runner in runners:
            runner.join()
