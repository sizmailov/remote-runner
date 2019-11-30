import os
import tempfile
import contextlib
from pathlib import Path
import threading
import logging


class ChangeDirectory:

    def __init__(self, dirname: Path = None):
        assert threading.current_thread() is threading.main_thread()
        assert dirname is None or dirname.is_dir()

        if dirname is None:
            dirname = Path.cwd().absolute()
        self.dirname = dirname
        self.prev_dir = Path.cwd().absolute()

    def __enter__(self):
        self.prev_dir = Path.cwd().absolute()
        os.chdir(str(self.dirname))

    def __exit__(self, exc_type, exc_val, exc_tb):
        os.chdir(str(self.prev_dir))


@contextlib.contextmanager
def ChangeToTemporaryDirectory():
    with tempfile.TemporaryDirectory() as temp_dir:
        with ChangeDirectory(Path(temp_dir)):
            yield None


def self_logger(self_or_class):
    if isinstance(self_or_class, type):
        klass = self_or_class
    else:
        klass = self_or_class.__class__
    class_name = klass.__name__
    class_module = klass.__module__
    return logging.getLogger(f"{class_module}.{class_name}")
