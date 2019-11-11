from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory
import signal
from ssh_common import ssh_worker_factory
import remote_runner
import logging

logger = logging.getLogger(remote_runner.__name__)

fmt = logging.Formatter('[%(name)s][%(funcName)s][%(asctime)-15s][%(relativeCreated)d][%(levelname)-6s]%(message)s')

ch = logging.FileHandler("example.log")
ch.setFormatter(fmt)

logger.setLevel(logging.DEBUG)
logger.addHandler(ch)


class MyTask(Task):
    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
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
        except StopCalculationError:
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


with ChangeToTemporaryDirectory():
    tasks = [
        MyTask(name="one"),
        MyTask(name="two")
    ]

    with ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        workers = [
            ssh_worker_factory(),
            ssh_worker_factory()
        ]
    killer = SelfKiller(1.0, signal.SIGINT)
    killer.start()
    try:
        with RaiseOnSignals():
            Pool(workers).run(tasks)
    except StopCalculationError:
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

    assert "" == Path("one/stderr").open().read()
    assert "" == Path("two/stderr").open().read()

    assert 143 == int(Path("one/exit_code").open().read().strip())
    assert 143 == int(Path("two/exit_code").open().read().strip())

    tmp_remote_dirs = list(
        d for d in Path(workers[0].remote_root.expanduser()).glob("*") if d.is_dir()
    )

    assert tmp_remote_dirs == []
