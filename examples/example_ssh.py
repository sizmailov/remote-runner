import sys
from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory


class MyTask(Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        time.sleep(1)
        print(self.name)


class MyExceptionalTask(Task):

    def __init__(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        raise RuntimeError("Remote runtime error")


def worker_factory():
    worker = SSHWorker(host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
"""
    return worker


with ChangeToTemporaryDirectory():
    Pool([
        worker_factory(),
        worker_factory()
    ]).run([
        MyTask(name="1"),
        MyTask(name="2")
    ])

    assert "1" in Path("1/stdout").open().read()
    assert "2" in Path("2/stdout").open().read()

    assert 0 == int(Path("1/exit_code").open().read().strip())
    assert 0 == int(Path("2/exit_code").open().read().strip())

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []

with ChangeToTemporaryDirectory():
    Pool([
        worker_factory(),
        worker_factory()
    ]).run([
        MyExceptionalTask(name="e1"),
        MyExceptionalTask(name="e2")
    ])

    assert "Remote runtime error" in Path("e1/stderr").open().read()
    assert "Remote runtime error" in Path("e2/stderr").open().read()

    assert 1 == int(Path("e1/exit_code").open().read().strip())
    assert 1 == int(Path("e2/exit_code").open().read().strip())

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
