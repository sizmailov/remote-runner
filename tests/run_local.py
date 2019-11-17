import os
from pathlib import Path

import remote_runner


class MyTask(remote_runner.Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        remote_runner.Task.__init__(self, wd=Path(name).absolute())

    def run(self):
        print(self.name)


wd = Path.cwd()

with remote_runner.utility.ChangeToTemporaryDirectory():
    tasks = [
        MyTask(name="one"),
        MyTask(name="two")
    ]

    with remote_runner.utility.ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        workers = [
            remote_runner.LocalWorker(),
            remote_runner.LocalWorker()
        ]

    remote_runner.Pool(workers).run(tasks)

    assert "one" in Path("one/stdout").open().read()
    assert "two" in Path("two/stdout").open().read()

    assert "" == Path("one/stderr").open().read()
    assert "" == Path("two/stderr").open().read()

    assert 0 == int(Path("one/exit_code").open().read().strip())
    assert 0 == int(Path("two/exit_code").open().read().strip())
