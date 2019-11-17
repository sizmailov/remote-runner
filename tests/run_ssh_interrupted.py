import os
import signal
import threading
import time
from pathlib import Path

import remote_runner
from ssh_common import ssh_worker_factory

remote_runner.log_to('remote-runner.log')


class MyTask(remote_runner.Task):
    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        remote_runner.Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        import sys
        try:
            print("calculation begin")
            total = 0
            desired = 10
            dt = 0.1
            while total < desired:
                time.sleep(dt)
                total += dt
                print(total)
            print("calculation end")
        except remote_runner.StopCalculationError:
            print("interrupted")
            raise
        else:
            print("normal termination", file=sys.stderr)


wd = Path.cwd()


class SelfKiller(threading.Thread):

    def __init__(self, delay, signum):
        self.delay = delay
        self.pid = os.getpid()
        self.signum = signum
        super().__init__()

    def run(self):
        time.sleep(self.delay)
        os.kill(self.pid, self.signum)


with remote_runner.utility.ChangeToTemporaryDirectory():
    tasks = [
        MyTask(name="one"),
        MyTask(name="two")
    ]

    with remote_runner.utility.ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        workers = [
            ssh_worker_factory(),
            ssh_worker_factory()
        ]
    killer = SelfKiller(2.0, signal.SIGINT)
    killer.start()
    try:
        with remote_runner.RaiseOnSignals():
            remote_runner.Pool(workers).run(tasks)
    except remote_runner.StopCalculationError:
        pass
    else:
        assert False, "Excepted StopCalculationError exception"

    killer.join()

    one_stdout = Path("one/stdout").open().read()
    two_stdout = Path("two/stdout").open().read()

    assert "calculation begin" in one_stdout
    assert "calculation begin" in two_stdout

    assert "interrupted" in one_stdout
    assert "interrupted" in two_stdout

    assert "calculation end" not in one_stdout
    assert "calculation end" not in two_stdout

    assert "StopCalculationError" in Path("one/stderr").open().read()
    assert "StopCalculationError" in Path("two/stderr").open().read()

    assert 1 == int(Path("one/exit_code").open().read().strip())
    assert 1 == int(Path("two/exit_code").open().read().strip())

    tmp_remote_dirs = list(
        d for d in Path(workers[0].remote_root.expanduser()).glob("*") if d.is_dir()
    )

    assert tmp_remote_dirs == []
