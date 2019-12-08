import remote_runner
import threading
import time
import os
import pytest
import signal


class SelfDoubleKiller(threading.Thread):

    def __init__(self, delay, signum):
        self.delay = delay
        self.pid = os.getpid()
        self.signum = signum
        super().__init__()

    def run(self):
        time.sleep(self.delay)
        os.kill(self.pid, self.signum)
        time.sleep(self.delay)
        os.kill(self.pid, self.signum)


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_survive_double_kill_15(signum):
    killer = SelfDoubleKiller(0.3, signum)
    with remote_runner.RaiseOnceOnSignals():
        try:
            killer.start()
            time.sleep(1.0)
        except remote_runner.StopCalculationError as e:
            try:
                time.sleep(1.0)
            except Exception as e:
                raise
        else:
            assert False, "Didn't stop"

        finally:
            killer.join()
