import sys
from remote_runner import *


class MyTask(Task):

    def __init__(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        print(threading.current_thread().name)


def worker_factory():
    worker = SSHWorker(host='localhost')
    worker.remote_user_rc = f"""
source {os.path.abspath(os.path.join(os.path.dirname(sys.executable), "activate"))}
export PYTHONPATH={os.path.dirname(__file__)}
"""
    return worker


Pool([
    worker_factory(),
    worker_factory()
]).run([
    MyTask(name="1"),
    MyTask(name="2")
])
