from .utility import ChangeDirectory
import time
from typing import List, Union
import queue
import hashlib
import threading
import multiprocessing
import subprocess
from pathlib import Path
import paramiko
import os
import shlex
import dill
from .errors import StopCalculationError, RaiseOnSignals


class Task:

    def __init__(self, wd: Path):
        assert wd.is_dir()
        self.wd = wd.absolute()
        self.state_filename = Path('state.dill')

    def save(self, filename: Path):
        tmp = Path(f"{filename}.bak")

        with tmp.open('wb') as out:
            self.state_filename = filename.name
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

    def __str__(self):
        return f"<{self.__class__.__name__} wd='{self.wd}'>"


class Worker:
    def run(self, task: Task):
        raise NotImplementedError()

    def stage_in(self, task: Task):
        raise NotImplementedError()

    def stage_out(self, task: Task):
        raise NotImplementedError()


class NullContextManager:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SSHWorker(Worker):
    ssh_config_file = "~/.ssh/config"
    rsync_to_remote_args = ['-a']
    rsync_to_local_args = ['-a']
    remote_user_rc = "# User remote initialization script"

    def __init__(self, host: str, remote_root: Path = None):
        self.host = host
        if remote_root is None:
            remote_root = Path("~/remote_tmp_root")
        self.remote_root: Path = remote_root
        self.remote_script_id = None
        self._ssh = None

    def remote_wd(self, local_wd: Path):
        m = hashlib.md5()
        m.update(str(local_wd.absolute()).encode('utf-8'))
        md5sum = m.hexdigest()
        return self.remote_root / md5sum

    @staticmethod
    def rsync(args, src, dst):
        rsync_cmd = ["rsync"] + args + [src, dst]
        subprocess.call(rsync_cmd)  # todo: add timeout and retry

    def remote_call(self, cmd: Union[List[str], str], stdin=None, sleep_time=0.05):
        channel = self.ssh.get_transport().open_session()  # type: paramiko.Channel
        if isinstance(cmd, str):
            cmd_line = cmd
        else:
            cmd_line = ' '.join(shlex.quote(str(x)) for x in cmd)  # shelx.join(cmd)
        channel.exec_command(cmd_line)
        if stdin is not None:
            inp = channel.makefile('wb', -1)
            inp.write(stdin)
            inp.flush()
            inp.close()
            channel.shutdown_write()

        out = b''
        err = b''

        buffer_size = 80
        while not channel.exit_status_ready():  # monitoring process
            while channel.recv_ready():
                out += channel.recv(buffer_size)
            while channel.recv_stderr_ready():
                err += channel.recv_stderr(buffer_size)
            time.sleep(sleep_time)
            if sleep_time < 1:
                sleep_time *= 2

        while channel.recv_ready():
            out += channel.recv(buffer_size)
        while channel.recv_stderr_ready():
            err += channel.recv_stderr(buffer_size)
        exit_code = channel.recv_exit_status()
        return exit_code, out, err

    def stage_in(self, task: Task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        self.remote_call(f'mkdir -p {remote}')
        self.rsync(self.rsync_to_remote_args, f"{local}/", f"{self.host}:{remote}/")

    def stage_out(self, task: Task):
        remote = self.copy_to_local(task)
        self.remote_call(f'rm -rf {remote}')

    def copy_to_local(self, task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        self.rsync(self.rsync_to_local_args, f"{self.host}:{remote}/", f"{local}/", )
        return remote

    def run(self, task: Task):
        self.stage_in(task)
        try:
            self.run_remotely(task)
        finally:
            self.stage_out(task)

    def remote_script_is_running(self):
        assert self.remote_script_id is not None
        ecode, stdout, stderr = self.remote_call(["ps", "-p", self.remote_script_id, "-o", "comm="])
        return ecode == 0

    def kill_remote_script(self, timeout=3):
        assert self.remote_script_id is not None

        self._kill_and_wait(15, timeout)

        if self.remote_script_is_running():
            self._kill_and_wait(9, timeout)

        assert not self.remote_script_is_running()

    def _kill_and_wait(self, signum, timeout):
        self.remote_call(["kill", f"-{signum}", self.remote_script_id])
        sleep_time = 0.05
        max_sleep_time = 30
        total = 0
        while self.remote_script_is_running() and total < timeout:
            time.sleep(sleep_time)
            total += sleep_time
            sleep_time = min(max_sleep_time, sleep_time * 2)

    def run_remotely(self, task: Task):
        self.start_remote_script(task)

        sleep_time = 0.5
        max_sleep_time = 30

        try:
            with self.remote_watcher(task):
                while self.remote_script_is_running():
                    time.sleep(sleep_time)
                    sleep_time = min(max_sleep_time, sleep_time * 2)

        except Exception:
            self.kill_remote_script()
            raise

    def remote_watcher(self, task: Task):
        return NullContextManager()

    def start_remote_script(self, task):
        script = self.generate_remote_script(task)

        ecode, stdout, stderr = self.remote_call(script)
        remote_pid = int(stdout.decode('utf-8').splitlines()[-1])
        self.remote_script_id = remote_pid

    def generate_remote_script(self, task):
        script = f"""
source /etc/profile
source ~/.profile

{self.remote_user_rc}

WORK_DIR={self.remote_wd(task.wd)}
cd "$WORK_DIR"

nohup python -c "
from remote_runner import *

with RaiseOnSignals():
    task = Task.load(Path('{shlex.quote(str(task.state_filename))}'))
    worker = LocalWorker()
    worker.run(task)
" > .startup.stdout 2>.startup.stderr &
echo $!

"""
        return script

    @property
    def ssh(self):
        if self._ssh is None or self._ssh_is_dead():
            self._ssh = self._create_ssh_client()
        return self._ssh

    def _ssh_is_dead(self):
        try:
            transport = self._ssh.get_transport()
            transport.send_ignore()
            return False
        except (AttributeError, paramiko.SSHException):
            return True

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

    def _create_ssh_client(self):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(**self._get_host_config())
        return client


class SyncRemoteFolder:
    def __init__(self, worker: SSHWorker, task: Task, sync_period: float):
        self.worker = worker
        self.task = task

        self.sync_period = sync_period
        self.task_complete = threading.Event()

    def __enter__(self):
        self.task_complete.clear()

        def sync():
            while not self.task_complete.is_set():  # todo: handle exiting
                self.worker.copy_to_local(self.task)
                self.task_complete.wait(timeout=self.sync_period)

        self.syncing_thread = threading.Thread(target=sync)
        self.syncing_thread.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.task_complete.set()
        self.syncing_thread.join()


class SyncSSHWorker(SSHWorker):
    def __init__(self, sync_period: float, *args, **kwargs):
        self.sync_period = sync_period
        super().__init__(*args, **kwargs)

    def remote_watcher(self, task: Task):
        return SyncRemoteFolder(worker=self, task=task, sync_period=self.sync_period)


class LocalWorker(Worker):
    def run(self, task: Task):
        # stderr/stdout are redirected to files and intentionally leaved open
        # to avoid higher-level output loss
        # This redirection is limited to process scope, therefore
        # time-overlapped calls to Worker.run() are prohibited within one process.
        # Process could be a standalone or multiprocessing.Process
        import sys
        sys.stdout = open("stdout", "w")
        sys.stderr = open("stderr", "w")

        with open("exit_code", "w") as exit_code:
            try:
                task.run()
            except Exception:
                exit_code.write("1")
                raise
            else:
                exit_code.write("0")

    def stage_in(self, task: Task):
        pass

    def stage_out(self, task: Task):
        pass


class Runner(multiprocessing.Process):
    def __init__(self, worker: Worker, tasks: 'multiprocessing.Queue[Task]'):
        super().__init__()
        self.worker = worker
        self.tasks = tasks

    def run(self):
        with RaiseOnSignals():
            try:
                while True:
                    task = self.tasks.get(block=False)
                    with ChangeDirectory(task.wd):
                        try:
                            self.worker.run(task)
                        except StopCalculationError:
                            break
                        except Exception:
                            import traceback
                            traceback.print_exc()
            except queue.Empty:
                pass


class Pool:
    def __init__(self, workers: List[Worker]):
        self.workers = workers

    def run(self, tasks: List[Task]):
        with multiprocessing.Manager() as manager:
            tasks_queue = manager.Queue(maxsize=len(tasks))

            def generate_runners():
                for worker in self.workers:
                    yield Runner(worker=worker, tasks=tasks_queue)

            stated_runners = []

            for task in tasks:
                tasks_queue.put(task)
            try:
                with RaiseOnSignals():
                    for runner in generate_runners():
                        runner.start()
                        stated_runners.append(runner)
                    for runner in stated_runners:
                        runner.join()
            finally:
                for runner in stated_runners:
                    if runner.is_alive():
                        runner.terminate()

                for runner in stated_runners:
                    runner.join()
