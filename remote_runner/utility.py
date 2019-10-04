import os
import tempfile
import stat
import shutil
import contextlib


class ChangeDirectory(object):
    def __init__(self, dir):
        self.new_dir = dir
        self.old_dir = os.path.abspath(os.path.curdir)

    def __enter__(self):
        os.chdir(self.new_dir)

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(self.old_dir)


class TemporaryDirectory(object):
    """ Mimic tempfile.TemporaryDirectory from python3 """

    def __init__(self):
        self.dir_name = None

    def __enter__(self):
        self.dir_name = tempfile.mkdtemp("remote_runner_")
        return self.dir_name

    def __exit__(self, exc_type, exc_val, exc_tb):
        def retry_with_chmod(func, path, exec_info):
            os.chmod(path, stat.S_IWRITE)
            func(path)

        if self.dir_name:
            shutil.rmtree(self.dir_name, onerror=retry_with_chmod)


@contextlib.contextmanager
def ChangeToTemporaryDirectory():
    with TemporaryDirectory() as temp_dir:
        with ChangeDirectory(temp_dir):
            yield None
