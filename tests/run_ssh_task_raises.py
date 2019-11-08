from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory
from ssh_common import ssh_worker_factory


class MyExceptionalTask(Task):

    def __init__(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        raise RuntimeError("Remote runtime error")


with ChangeToTemporaryDirectory():
    Pool([
        ssh_worker_factory(),
        ssh_worker_factory()
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
        d for d in Path(ssh_worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
