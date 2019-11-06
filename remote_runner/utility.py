import os
import tempfile
import contextlib
from pathlib import Path


class ChangeDirectory:

    def __init__(self, dirname: Path = None):
        assert dirname.is_dir()

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
