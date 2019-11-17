from pathlib import Path

import remote_runner


class MyLocalTask(remote_runner.Task):
    def __init__(self, wd: remote_runner.Path, message: str):
        wd.mkdir()
        self.message = message
        remote_runner.Task.__init__(self, wd)

    def run(self):
        with open("data", "w") as f:
            f.write(self.message)


def test_local_worker():
    with remote_runner.utility.ChangeToTemporaryDirectory():
        tasks = [
            MyLocalTask(wd=Path("t1"), message='1'),
            MyLocalTask(wd=Path("t2"), message='2'),
            MyLocalTask(wd=Path("t3"), message='3'),
        ]
        worker = remote_runner.LocalWorker()
        pool = remote_runner.Pool([worker])
        pool.run(tasks)

        assert Path("t1", "data").open().read() == "1"
        assert Path("t2", "data").open().read() == "2"
        assert Path("t3", "data").open().read() == "3"
