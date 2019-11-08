from remote_runner import *
from remote_runner.utility import ChangeToTemporaryDirectory
from ssh_common import ssh_worker_factory


class MyTask(Task):

    def __init__(self, name):
        self.name = name
        if not os.path.exists(name):
            os.mkdir(name)
        Task.__init__(self, wd=Path(name).absolute())
        self.save(Path(name, self.state_filename))

    def run(self):
        time.sleep(1)
        print(self.name)


with ChangeToTemporaryDirectory():
    Pool([
        ssh_worker_factory(),
        ssh_worker_factory()
    ]).run([
        MyTask(name="1"),
        MyTask(name="2")
    ])

    assert "1" in Path("1/stdout").open().read()
    assert "2" in Path("2/stdout").open().read()

    assert 0 == int(Path("1/exit_code").open().read().strip())
    assert 0 == int(Path("2/exit_code").open().read().strip())

    # remote root is cleaned
    tmp_remote_dirs = list(
        d for d in Path(ssh_worker_factory().remote_root.expanduser()).glob("*") if d.is_dir()
    )
    assert tmp_remote_dirs == []
