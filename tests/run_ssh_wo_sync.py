import os
import time
from pathlib import Path

import remote_runner
from tests.ssh_common import ssh_worker_factory

remote_runner.log_to('remote-runner.log', level="DEBUG")


class MyTask(remote_runner.Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        remote_runner.Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        time.sleep(1)
        print(self.name)


wd = Path.cwd()

with remote_runner.utility.ChangeToTemporaryDirectory():
    tasks = [
        MyTask(name="1"),
        MyTask(name="2")
    ]

    with remote_runner.utility.ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        remote_runner.Pool([
            ssh_worker_factory(),
            ssh_worker_factory()
        ]).run(tasks)

    assert "1" in Path("1/stdout").open().read()
    assert "2" in Path("2/stdout").open().read()

    assert 0 == int(Path("1/exit_code").open().read().strip())
    assert 0 == int(Path("2/exit_code").open().read().strip())

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(ssh_worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
