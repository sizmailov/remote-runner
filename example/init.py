import remote_runner
import shutil
from pathlib import Path


class MyTask(remote_runner.Task):

    def __init__(self, num):
        work_dir = Path(f"{num:02d}")
        if work_dir.exists():
            shutil.rmtree(str(work_dir))

        work_dir.mkdir()

        # avoid super() call due to  https://github.com/uqfoundation/dill/issues/300
        remote_runner.Task.__init__(self, wd=work_dir)

    def run(self):
        import os
        import time
        print(os.path.abspath(os.curdir))
        time.sleep(1)
        for k, v in os.environ.items():
            print(f"{k} = {v}")


if __name__ == "__main__":
    for i in range(10):
        task = MyTask(i)
        # save to task work dir
        task.save(task.wd / task.state_filename)
