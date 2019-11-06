from remote_runner import *


class MyTask(Task):

    def run(self):
        print(threading.current_thread().name)


Pool([
    LocalWorker(),
    LocalWorker()
]).run([
    MyTask(wd=Path.cwd()),
    MyTask(wd=Path.cwd())
])
