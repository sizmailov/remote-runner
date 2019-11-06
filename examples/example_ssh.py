import sys
from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory


class MyTask(Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        subprocess.call(["readlink", "-m", name])
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        print(self.name)


class MyExceptionalTask(Task):

    def __init__(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        subprocess.call(["readlink", "-m", name])
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        raise RuntimeError("Remote runtime error")


def worker_factory():
    worker = SSHWorker(host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
export PYTHONPATH={os.path.dirname(__file__)}
"""
    return worker


with ChangeToTemporaryDirectory():
    os.system("pwd")
    Pool([
        worker_factory(),
        worker_factory()
    ]).run([
        MyTask(name="1"),
        MyTask(name="2")
    ])
    time.sleep(1.0)
    subprocess.call(["sync"])  # prevent spurious false-positive assertions
    os.system("ls")
    os.system("ls 1 2")
    assert "1" in Path("1/stdout").open().read()
    assert "2" in Path("2/stdout").open().read()
    # remote root is cleaned
    assert list(Path(worker_factory().remote_root).glob("*")) == []

with ChangeToTemporaryDirectory():
    os.system("pwd")
    Pool([
        worker_factory(),
        worker_factory()
    ]).run([
        MyExceptionalTask(name="e1"),
        MyExceptionalTask(name="e2")
    ])

    time.sleep(1.0)
    subprocess.call(["sync"])  # prevent spurious false-positive assertions
    os.system("ls")
    os.system("ls e1 e2")
    assert "Remote runtime error" in Path("e1/stderr").open().read()
    assert "Remote runtime error" in Path("e2/stderr").open().read()
    # remote root is cleaned
    assert list(Path(worker_factory().remote_root).glob("*")) == []
