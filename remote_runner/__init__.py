from .utility import ChangeDirectory, self_logger as _logger
import time
from typing import List, Union, Tuple
import queue
import hashlib
import threading
import multiprocessing
import subprocess
from pathlib import Path
import paramiko
import os
import re
import shlex
import dill
from .errors import StopCalculationError, RaiseOnceOnSignals


class Task:
    _protected_methods = ["run", "save"]

    def __init__(self, wd: Path):
        assert wd.is_dir()
        self.wd = wd.absolute()
        self.state_filename = Path('state.dill')

    def save(self, filename: Path):
        tmp = Path(f"{filename}.bak")

        with tmp.open('wb') as out:
            self.state_filename = Path(filename.name)
            dill.dump(self, out)
            out.close()
        tmp.rename(filename)
        _logger(self).info(f"{self} saved to {filename}")

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
        return f"{self.__class__.__name__}(wd='{self.wd}')"

    @property
    def name_or_wd(self) -> str:
        try:
            return self.name
        except AttributeError:
            return self.wd.name

    def __setattr__(self, key, value):
        if key in self._protected_methods and not callable(value):
            raise RuntimeError(f"Attempting to override Task.{key}() with non-callable attribute")
        super().__setattr__(key, value)


class Worker:
    def run(self, task: Task):
        raise NotImplementedError()


class NullContextManager:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class RemoteScriptError(Exception):
    def __init__(self, script: str, exit_code: int, stdout: bytes, stderr: bytes):
        self.command = script
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

    def __str__(self):
        return f"""Command exited with {self.exit_code}
Command:
{self.command}

Stdout:
{self.stdout.decode('utf-8')}

Stderr:
{self.stderr.decode('utf-8')}

"""


class RemoteWorker(Worker):
    remote_user_rc = "# User remote initialization script"

    def __init__(self, remote_user_rc: str = None):
        if remote_user_rc is not None:
            self.remote_user_rc = remote_user_rc

    def stage_in(self, task: Task):
        raise NotImplementedError()

    def stage_out(self, task: Task):
        raise NotImplementedError()

    def run(self, task: Task):
        _logger(self).info(f"{self}:{task}")
        _logger(self).debug(f"{self}:{task} stage in BEGIN")
        self.stage_in(task)
        _logger(self).debug(f"{self}:{task} stage in END ")
        try:
            _logger(self).debug(f"{self}:{task} run remotely BEGIN")
            self.run_remotely(task)
            _logger(self).debug(f"{self}:{task} run remotely END")
        finally:
            _logger(self).debug(f"{self}:{task} stage out BEGIN")
            self.stage_out(task)
            _logger(self).debug(f"{self}:{task} stage out END")

    def run_remotely(self, task: Task):
        self.start_remote_script(task)
        _logger(self).info(f"{self}:{task} started")

        sleep_time = 0.5
        max_sleep_time = 15

        try:
            with self.remote_watcher(task):
                while self.remote_script_is_running():
                    time.sleep(sleep_time)
                    sleep_time = min(max_sleep_time, sleep_time * 2)

        except Exception as e:
            _logger(self).error(f"Exception occurred during remote task processing {e}")
            _logger(self).debug("Going to kill remote script")
            self.kill_remote_script()
            raise

    def remote_watcher(self, task: Task):
        raise NotImplementedError()

    def remote_script_is_running(self):
        raise NotImplementedError()

    def start_remote_script(self, task):
        raise NotImplementedError()

    def kill_remote_script(self):
        raise NotImplementedError()

    def generate_remote_script(self, task: Task):
        raise NotImplementedError()


class SSHWorker(RemoteWorker):
    ssh_config_file = "~/.ssh/config"
    rsync_to_remote_args = ['-a']
    rsync_to_local_args = ['-a']

    def __init__(self, host: str, remote_root: Path = None,
                 rsync_to_remote_args: List[str] = None, rsync_to_local_args: List[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if rsync_to_local_args is not None:
            self.rsync_to_local_args = rsync_to_local_args
        if rsync_to_remote_args is not None:
            self.rsync_to_remote_args = rsync_to_remote_args
        if remote_root is None:
            remote_root = Path("~/remote_tmp_root")

        self.host = host
        self._remote_root: Path = remote_root
        self.remote_script_id = None
        self._ssh = None

    @property
    def remote_root(self):
        if str(self._remote_root).startswith("~"):
            ecode, stdout, stderr = self.remote_call("echo ~", output_encoding="utf-8")
            if ecode == 0:
                remote_home = stdout.strip()
                self._remote_root = Path(str(self._remote_root).replace("~", remote_home, 1))
                _logger(self).debug(f"Remote home is `{self._remote_root}`")
            else:
                raise RuntimeError("Can't find remote home")

        return self._remote_root

    def __str__(self):
        return f"{self.__class__.__name__}({self.host})"

    def remote_wd(self, local_wd: Path):
        m = hashlib.md5()
        m.update(str(local_wd.absolute()).encode('utf-8'))
        md5sum = m.hexdigest()
        return self.remote_root / md5sum

    @classmethod
    def rsync(cls, args, src, dst):
        rsync_cmd = ["rsync"] + args + [src, dst]
        _logger(cls).info(" ".join(rsync_cmd))
        subprocess.call(rsync_cmd)  # todo: add timeout and retry

    def remote_call(self, cmd: Union[List[str], str], stdin=None, sleep_time=0.05, output_encoding=None):
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
        if output_encoding is not None:
            out = out.decode(output_encoding)
            err = err.decode(output_encoding)
        return exit_code, out, err

    def stage_in(self, task: Task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        _logger(self).info(f"{self}:{task}")
        self.remote_call(f'mkdir -p {remote}')
        self.rsync(self.rsync_to_remote_args, f"{local}/", f"{self.host}:{remote}/")

    def stage_out(self, task: Task):
        _logger(self).info(f"{self}:{task}")
        remote = self.copy_to_local(task)
        self.remote_call(f'rm -rf {remote}')

    def copy_to_local(self, task):
        local = task.wd.absolute()
        remote = self.remote_wd(local)
        self.rsync(self.rsync_to_local_args, f"{self.host}:{remote}/", f"{local}/", )
        return remote

    def remote_script_is_running(self):
        assert self.remote_script_id is not None
        ecode, stdout, stderr = self.remote_call(["ps", "-p", self.remote_script_id, "-o", "comm="])
        _logger(self).debug(f"{self}:pid={self.remote_script_id} is {'NOT ' if ecode else ''}running")
        return ecode == 0

    def kill_remote_script(self, timeout=3):
        _logger(self).debug(f"Going to kill {self.remote_script_id}")
        assert self.remote_script_id is not None

        self._kill_and_wait(15, timeout)

        if self.remote_script_is_running():
            self._kill_and_wait(9, timeout)

        assert not self.remote_script_is_running()

    def _kill_and_wait(self, signum, timeout):
        _logger(self).error(f"{self}: kill {self.remote_script_id} with {signum}")
        self.remote_call(["kill", f"-{signum}", self.remote_script_id])
        sleep_time = 0.05
        max_sleep_time = 30
        total = 0
        while self.remote_script_is_running() and total < timeout:
            time.sleep(sleep_time)
            total += sleep_time
            sleep_time = min(max_sleep_time, sleep_time * 2)

    def remote_watcher(self, task: Task):
        return NullContextManager()

    def start_remote_script(self, task):
        script = self.generate_remote_script(task)

        ecode, stdout, stderr = self.remote_call(script)
        if ecode != 0:
            raise RemoteScriptError(script, ecode, stdout, stderr)

        remote_pid = int(stdout.decode('utf-8').splitlines()[-1])
        self.remote_script_id = remote_pid

    def generate_remote_script(self, task: Task):
        script = f"""
source /etc/profile
source ~/.profile

{self.remote_user_rc}

WORK_DIR={self.remote_wd(task.wd)}
cd "$WORK_DIR"

nohup python -c "
import remote_runner
from remote_runner import *

with RaiseOnceOnSignals():
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
        super().__init__(*args, **kwargs)
        self.sync_period = sync_period

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


class PbsWorker(RemoteWorker):

    def generate_remote_script(self, task: Task):
        raise NotImplementedError()

    def __init__(self, resources: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resources = resources
        self.remote_script_id = None

    def pbs_server_call(self, cmd: List[str], stdin: str = None) -> Tuple[int, str, str]:
        raise NotImplementedError()

    def remote_watcher(self, task: Task):
        raise NotImplementedError()

    def remote_script_is_running(self):
        return self.get_job_state() in ["Q", "R", "E"]  # queued, running or exiting

    def start_remote_script(self, task: Task):
        qsub_command = self.qsub_command(task)
        ecode, stdout, stderr = self.pbs_server_call(qsub_command,
                                                     stdin=self.generate_remote_script(task))
        if ecode != 0:
            raise RuntimeError(f"Can't send job {qsub_command}\n"
                               f"Stderr:\n {stderr}")
        self.remote_script_id = stdout.strip()

    def kill_remote_script(self):
        job_state = self.get_job_state()
        _logger(self).debug(f"PBS job {self.remote_script_id} has status {job_state}")

        if job_state in ["Q", "R"]:
            _logger(self).debug(f"Trying to kill job {self.remote_script_id}")
            ecode, stdout, stderr = self.pbs_server_call(["qdel", self.remote_script_id])

            if ecode not in [0, 170] and self.get_job_state() in ["R", "Q"]:
                _logger(self).error(f"qsub returned with {ecode}")
                raise RuntimeError(f"Can't qdel job {self.remote_script_id}")

            _logger(self).debug(f"qdel {self.remote_script_id} - success")

            max_time_to_sleep = 10.0
            time_to_sleep = 0.1
            while self.get_job_state() not in ["C", None]:
                _logger(self).debug(f"Waiting for {self.remote_script_id} termination...")
                time.sleep(time_to_sleep)  # todo: remove magic constant
                time_to_sleep = min(max_time_to_sleep, time_to_sleep * 2)

    def get_job_state(self):
        from .pbs import QStatParser
        if self.remote_script_id is None:
            return None
        qstat_parser = QStatParser()
        ecode, stdout, stderr = self.pbs_server_call(["qstat", "-f", self.remote_script_id])
        if ecode != 0:
            return None
        try:
            qstat = qstat_parser.parse(stdout)
            return qstat[self.remote_script_id]["job_state"]
        except KeyError:
            return None

    def qsub_command(self, task):
        return ["qsub", "-l", self.resources, "-N", task.name_or_wd]


class SSHPbsWorker(PbsWorker, SSHWorker):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def pbs_server_call(self, cmd: List[str], stdin=None) -> Tuple[int, bytes, bytes]:
        return self.remote_call(cmd, stdin=stdin, output_encoding="utf-8")

    def remote_watcher(self, task: Task):
        return NullContextManager()

    def generate_remote_script(self, task: Task):
        script = f"""
source /etc/profile
source ~/.profile

{self.remote_user_rc}

WORK_DIR={self.remote_wd(task.wd)}
cd "$WORK_DIR"

exec python -c "
import remote_runner
from remote_runner.errors import RaiseOnceOnSignals
from pathlib import Path

with RaiseOnceOnSignals():
    task = remote_runner.Task.load(Path('{shlex.quote(str(task.state_filename))}'))
    worker = remote_runner.LocalWorker()
    worker.run(task)
"
"""
        return script

    def qsub_command(self, task):
        return super().qsub_command(task) + [
            '-e', self.remote_wd(task.wd) / ".pbs.stderr",
            '-o', self.remote_wd(task.wd) / ".pbs.stdout"
        ]


class LocalPbsWorker(PbsWorker):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def generate_remote_script(self, task: Task):
        script = f"""
source /etc/profile
source ~/.profile

{self.remote_user_rc}

WORK_DIR={task.wd}
cd "$WORK_DIR"

exec python -c "
import remote_runner
from remote_runner.errors import RaiseOnceOnSignals
from pathlib import Path

with RaiseOnceOnSignals():
    task = remote_runner.Task.load(Path('{shlex.quote(str(task.state_filename))}'))
    worker = remote_runner.LocalWorker()
    worker.run(task)
"
    """
        return script

    def pbs_server_call(self, cmd: List[str], stdin: str = None) -> Tuple[int, str, str]:
        if stdin is not None:
            inp = subprocess.PIPE
            stdin = stdin.encode('utf-8')
        else:
            inp = None
        proc = subprocess.Popen(cmd, stdin=inp, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, stderr = proc.communicate(input=stdin)
        if not stdout:
            stdout = b""
        if not stderr:
            stderr = b""
        return proc.returncode, stdout.decode("utf-8"), stderr.decode("utf-8")

    def remote_watcher(self, task: Task):
        return NullContextManager()

    def stage_in(self, task: Task):
        pass

    def stage_out(self, task: Task):
        pass

    def qsub_command(self, task):
        return super().qsub_command(task) + [
            '-e', task.wd / ".pbs.stderr",
            '-o', task.wd / ".pbs.stdout"
        ]


class LocalSlurmWorker(RemoteWorker):

    def __init__(self, remote_user_rc: str = None, sbatch_args: List[str] = None):
        super(LocalSlurmWorker, self).__init__(remote_user_rc)
        self.sbatch_args: List[str] = sbatch_args or []

    def stage_in(self, task: Task):
        pass

    def stage_out(self, task: Task):
        pass

    def remote_watcher(self, task: Task):
        return NullContextManager()

    def remote_script_is_running(self):
        state = self.get_job_state()
        return state in ["PENDING", "RUNNING", "STAGE_OUT", "COMPLETING"]

    def start_remote_script(self, task):
        sbatch_command = self.sbatch_command(task)
        p = subprocess.run(
            sbatch_command,
            input=self.generate_remote_script(task),
            encoding='utf-8',
            capture_output=True
        )

        if p.returncode != 0:
            raise RuntimeError(f"Can't send job {sbatch_command}\n"
                               f"Stderr:\n {p.stderr}")

        self.job_id = self.parse_job_id(p.stdout)

        # Immediate subsequent call to sacct returns nothing
        # wait for a second to circumvent this problem
        time.sleep(1.0)

    def parse_job_id(self, sbatch_stdout: str) -> int:
        m = re.search(
            r"Submitted batch job (?P<job_id>\d+)",
            sbatch_stdout
        )
        if not m:
            raise RuntimeError(f"Can't parse job_id from sbatch output:\n{sbatch_stdout}")
        job_id = int(m.group("job_id"))
        return job_id

    def kill_remote_script(self):
        subprocess.call([
            "scancel",
            "--full",
            f"{self.job_id}"
        ])

    def generate_remote_script(self, task: Task):
        return f"""\
#!/bin/bash

source /etc/profile
source ~/.profile

{self.remote_user_rc}

WORK_DIR={task.wd}
cd "$WORK_DIR"

exec python -c "
import remote_runner
from remote_runner.errors import RaiseOnceOnSignals
from pathlib import Path

with RaiseOnceOnSignals():
    task = remote_runner.Task.load(Path('{shlex.quote(str(task.state_filename))}'))
    worker = remote_runner.LocalWorker()
    worker.run(task)
"
    """

    def get_job_state(self) -> str:
        cmd = [
            "sacct",
            "--job", f"{self.job_id}.batch",
            "--noheader",
            "--format=state",
            "--parsable2"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True
        )

        for i in range(2):
            if result.stdout:
                break
            else:
                time.sleep(0.5)

            result = subprocess.run(
                cmd,
                capture_output=True
            )

        return (result.stdout or b"").decode('utf-8').strip()

    def sbatch_command(self, task: Task) -> List[str]:
        return [
                   "sbatch",
                   f"--job-name={task.name_or_wd}",
                   f"--error={task.wd}/.slurm.stderr",
                   f"--output={task.wd}/.slurm.stdout"
               ] + self.sbatch_args


class Runner(multiprocessing.Process):
    def __init__(self, worker: Worker, tasks: 'multiprocessing.Queue[Path]'):
        super().__init__()
        self.worker = worker
        self.tasks = tasks

    def __str__(self):
        return f"{self.__class__.__name__}(worker={self.worker}, name={self.name})"

    def run(self):
        with RaiseOnceOnSignals():
            try:
                while True:
                    _logger(self).info(f"{self} start tasks processing")
                    task_path = self.tasks.get(block=False)
                    task = Task.load(task_path)
                    with ChangeDirectory(task.wd):
                        try:
                            _logger(self).info(f"{self}: starting {task}")
                            self.worker.run(task)
                        except StopCalculationError as e:
                            _logger(self).warning(f"{self}: {task} raised {e}")
                            break
                        except Exception as e:
                            _logger(self).error(f"{self}: {task} raised {e}")
                            import traceback
                            traceback.print_exc()
                        finally:
                            _logger(self).info(f"{self}: {task} done")
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
                path_to_task_file = task.wd / task.state_filename
                task.save(path_to_task_file)
                tasks_queue.put(path_to_task_file)
            try:
                with RaiseOnceOnSignals():
                    for runner in generate_runners():
                        runner.start()
                        stated_runners.append(runner)
                        _logger(self).info(f"{runner} spawned")
                    for runner in stated_runners:
                        runner.join()
            except Exception as e:
                _logger(self).error(f"Exception occurred during task processing: {e}")
                raise
            finally:
                for runner in stated_runners:
                    if runner.is_alive():
                        _logger(self).warning(f"Sending SIGTERM to {runner} from {self}")
                        runner.terminate()

                for runner in stated_runners:
                    runner.join()
                    _logger(self).info(f"{runner} joined")


def log_to(filename: Union[str, Path] = None, mode: str = None, fmt: str = None, level=None):
    import logging
    if fmt is None:
        fmt = '%(asctime)-15s [%(relativeCreated)7d] ' \
              '%(levelname)8s - %(name)s.%(funcName)s:%(lineno)d - ' \
              '%(message)s'
    if filename is None:
        filename = "remote-runner.log"
    if mode is None:
        mode = "a"
    if level is None:
        level = logging.INFO

    logger = logging.getLogger(__name__)

    formatter = logging.Formatter(fmt)

    ch = logging.FileHandler(filename=str(filename), mode=mode)
    ch.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(ch)
