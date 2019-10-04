import remote_runner
import subprocess
import os
import glob
import sys


def test_localhost_successful_run():
    with remote_runner.ChangeToTemporaryDirectory():
        with open("init.py", "w") as f:
            f.write("""
import remote_runner
import os

class MyTask(remote_runner.RemoteTask):

    def __init__(self, x):
        import os
        if not os.path.exists("%02d" % x):
            os.mkdir("%02d" % x)
        remote_runner.RemoteTask.__init__(self, name=str(x), wd="%02d" % x)

    def run(self):
        import os
        print(os.environ["REMOTE_SECRET"])
        
for i in range(1):
    task = MyTask(i)
    task.save_state(os.path.join(task.wd, task.default_save_filename))
""")

        with open("run.py", "w") as f:
            f.write("""
import glob
import remote_runner
remote_tmp_root = "~/remote_tmp_root"

python_env = [
    "source 'PROJECT_ROOT/venv/bin/activate'",
    "export PYTHONPATH=PROJECT_ROOT",
    "export REMOTE_SECRET=FOOBAR"
]

pool = remote_runner.RemotePool([
    {
        "host": 'localhost',
        "remote_tmp_root": remote_tmp_root,
        "remote_environment_setup": python_env
    }
])

run_args = [
    {"state": state, "rsync_period": 5 * 60}
    for i, state in enumerate(sorted(glob.glob("*/state.dill")))
]
try:
    pool.run(run_args)
except:
    pool.close()
                """.replace("PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))))

        subprocess.check_call([sys.executable, "init.py"])
        os.remove("init.py")
        subprocess.check_call([sys.executable, "run.py"])

        assert glob.glob(os.path.expanduser("~/remote_tmp_root/*")) == []

        for i in range(1):
            with open("%02d/stdout"%i, 'r') as f:
                assert f.read().strip() == "FOOBAR"


def test_localhost_unsuccessful_run():
    with remote_runner.ChangeToTemporaryDirectory():
        with open("init.py", "w") as f:
            f.write("""
import remote_runner
import os

class MyTask(remote_runner.RemoteTask):

    def __init__(self, x):
        import os
        if not os.path.exists("%02d" % x):
            os.mkdir("%02d" % x)
        remote_runner.RemoteTask.__init__(self, name=str(x), wd="%02d" % x)

    def run(self):
        raise RuntimeError("Exception raised in MyTask.run()")

for i in range(1):
    task = MyTask(i)
    task.save_state(os.path.join(task.wd, task.default_save_filename))
""")

        with open("run.py", "w") as f:
            f.write("""
import glob
import remote_runner
remote_tmp_root = "~/remote_tmp_root"

python_env = [
    "source 'PROJECT_ROOT/venv/bin/activate'",
    "export PYTHONPATH=PROJECT_ROOT",
    "export REMOTE_SECRET=FOOBAR"
]

pool = remote_runner.RemotePool([
    {
        "host": 'localhost',
        "remote_tmp_root": remote_tmp_root,
        "remote_environment_setup": python_env
    }
])

run_args = [
    {"state": state, "rsync_period": 5 * 60}
    for i, state in enumerate(sorted(glob.glob("*/state.dill")))
]
try:
    pool.run(run_args)
except:
    pool.close()
                """.replace("PROJECT_ROOT", os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))))

        subprocess.check_call([sys.executable, "init.py"])
        os.remove("init.py")
        subprocess.check_call([sys.executable, "run.py"])

        assert glob.glob(os.path.expanduser("~/remote_tmp_root/*")) == []

        for i in range(1):
            with open("%02d/stderr"%i, 'r') as f:
                assert "Exception raised in MyTask.run()" in f.read()

            with open("%02d/stdout"%i, 'r') as f:
                assert f.read().strip() == ""
