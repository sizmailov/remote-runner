import pytest
import remote_runner
from pathlib import Path


class MyTask(remote_runner.Task):
    def run(self):
        print("hi")

    def save(self, filename: Path):
        print("hi")


def test_task_run_override_with_non_callable():
    task = MyTask(Path("./"))
    with pytest.raises(RuntimeError):
        task.run = "hi"


def test_task_save_override_with_non_callable():
    task = MyTask(Path("./"))
    with pytest.raises(RuntimeError):
        task.save = "hi"


def test_task_hello_assign_non_callable():
    task = MyTask(Path("./"))
    task.hello = "hi"
