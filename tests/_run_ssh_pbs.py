import os
from pathlib import Path

import remote_runner
import logging

# logging.basicConfig(level=logging.DEBUG)

class MyTask(remote_runner.Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        remote_runner.Task.__init__(self, wd=Path(name).absolute())

    def run(self):
        import socket
        import time
        print(self.name)
        time.sleep(30)
        print(socket.gethostname())


wd = Path.cwd()

with remote_runner.utility.ChangeDirectory(Path("TMP")):
    tasks = [
        MyTask(name="one"),
        MyTask(name="two")
    ]

    with remote_runner.utility.ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        workers = [
            remote_runner.SSHPbsWorker(host="bionmr", resources="nodes=1:ppn=1", remote_user_rc="""
unset PYTHONPATH
source ~/venv-3.8/bin/activate
""")
        ]

    remote_runner.Pool(workers).run(tasks)

    stdout_1 = Path("one/stdout").open().read()
    stdout_2 = Path("two/stdout").open().read()

    assert "one" in stdout_1
    assert "two" in stdout_2

    assert "bionmr-mom-00" in stdout_1
    assert "bionmr-mom-00" in stdout_2

    assert "" == Path("one/stderr").open().read()
    assert "" == Path("two/stderr").open().read()

    assert 0 == int(Path("one/exit_code").open().read().strip())
    assert 0 == int(Path("two/exit_code").open().read().strip())
