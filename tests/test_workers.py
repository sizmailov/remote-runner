from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory


class MyLocalTask(Task):
    def __init__(self, wd: Path, message: str):
        wd.mkdir()
        self.message = message
        Task.__init__(self, wd)

    def run(self):
        with open("data", "w") as f:
            f.write(self.message)


def test_local_worker():
    with ChangeToTemporaryDirectory():
        tasks = [
            MyLocalTask(wd=Path("t1"), message='1'),
            MyLocalTask(wd=Path("t2"), message='2'),
            MyLocalTask(wd=Path("t3"), message='3'),
        ]
        worker = LocalWorker()
        pool = Pool([worker])
        pool.run(tasks)

        assert Path("t1", "data").open().read() == "1"
        assert Path("t2", "data").open().read() == "2"
        assert Path("t3", "data").open().read() == "3"
