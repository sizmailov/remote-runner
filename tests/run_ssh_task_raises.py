import remote_runner
from tests.ssh_common import ssh_worker_factory
import os
from pathlib import Path


class MyExceptionalTask(remote_runner.Task):

    def __init__(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        remote_runner.Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        raise RuntimeError("Remote runtime error")


wd = Path.cwd()

with remote_runner.utility.ChangeToTemporaryDirectory():
    tasks = [
        MyExceptionalTask(name="e1"),
        MyExceptionalTask(name="e2")
    ]
    with remote_runner.utility.ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        remote_runner.Pool([
            ssh_worker_factory(),
            ssh_worker_factory()
        ]).run(tasks)

    assert "Remote runtime error" in Path("e1/stderr").open().read()
    assert "Remote runtime error" in Path("e2/stderr").open().read()

    assert 1 == int(Path("e1/exit_code").open().read().strip())
    assert 1 == int(Path("e2/exit_code").open().read().strip())

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(ssh_worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
