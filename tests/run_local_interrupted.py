from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory
import signal


class MyTask(Task):
    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())

    def run(self):
        import sys
        try:
            print("calculation begin")
            total = 0
            desired = 2
            dt = 0.5
            while total < desired:
                time.sleep(dt)
                total += dt
            print("calculation end")
        except StopCalculationError:
            print("interrupted")
            raise
        else:
            print("normal termination", file=sys.stderr)


wd = Path.cwd()


class SelfKiller(threading.Thread):

    def __init__(self, signum):
        self.signum = signum
        super().__init__()

    def run(self):
        time.sleep(0.6)
        os.kill(os.getpid(), self.signum)


with ChangeToTemporaryDirectory():
    tasks = [
        MyTask(name="one"),
        MyTask(name="two")
    ]

    killer = SelfKiller(signal.SIGTERM)

    with ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        workers = [
            LocalWorker(),
            LocalWorker()
        ]

    killer.start()
    try:
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

    assert 1 == int(Path("one/exit_code").open().read().strip())
    assert 1 == int(Path("two/exit_code").open().read().strip())
