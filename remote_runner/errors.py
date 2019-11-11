import signal


class StopCalculationError(Exception):
    """Emitted to interrupt calculation"""

    def __str__(self):
        return "StopCalculationError"


class RaiseOnSignals:

    def __enter__(self):
        self._raise_on_signals()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._reset_to_default()

    @staticmethod
    def _handler(signum, stack_frame):
        raise StopCalculationError()

    @staticmethod
    def _raise_on_signals(signums=(signal.SIGTERM, signal.SIGINT)):
        for signum in signums:
            signal.signal(signum, RaiseOnSignals._handler)

    @staticmethod
    def _reset_to_default(signums=(signal.SIGTERM, signal.SIGINT)):
        for signum in signums:
            signal.signal(signum, signal.SIG_DFL)
