import random
from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory
from ssh_common import sync_ssh_worker_factory


class MyTask(Task):

    def __init__(self, name, data):
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.data = data
        self.save(Path(name, self.state_filename))

    def run(self):
        import time
        with open("output", "w", 1) as fout:
            for x in self.data:
                fout.write(f"{x}")
                fout.flush()
                time.sleep(3.0 / len(self.data))


def read_file_content(filename):
    try:
        with open(filename) as f:
            return f.read()
    except OSError as e:
        return ""


class KeepReading(threading.Thread):

    def __init__(self, filename: str, period: float):
        super().__init__()
        self.stop_reading = threading.Event()
        self.period = period
        self.data = []
        self.filename = filename

    def run(self):
        self.stop_reading.clear()
        while not self.stop_reading.is_set():
            self.data.append(read_file_content(self.filename))
            self.stop_reading.wait(self.period)
        self.data.append(read_file_content(self.filename))


wd = Path.cwd()

with ChangeToTemporaryDirectory():
    N = 1000
    random_data = [random.randint(0, 100) for i in range(N)]

    task_name = "syncing_task"
    reader = KeepReading(Path(task_name, "output"), period=0.4)

    reader.start()
    worker = sync_ssh_worker_factory()

    tasks = [
        MyTask(name=task_name, data=random_data)
    ]

    with ChangeDirectory(wd):  # cd back to avoid .coverage.* files loss
        Pool([
            worker,
        ]).run(tasks)

    reader.stop_reading.set()
    reader.join()

    prev = reader.data[0]
    n_diffs = 0
    for curr in reader.data[1:]:
        assert curr.startswith(prev)
        if curr != prev:
            n_diffs += 1
        prev = curr

    assert n_diffs > 3  # output was copied multiple times

    expected_text = "".join(str(x) for x in random_data)
    assert expected_text == reader.data[-1]

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(worker.remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
