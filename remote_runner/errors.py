import signal


class StopCalculationError(Exception):
    """Emitted to interrupt calculation"""

    def __str__(self):
        return "StopCalculationError"


class RaiseOnceOnSignals:

    def __init__(self, signums=(signal.SIGTERM, signal.SIGINT), restore_at_exit=False):
        self.signums = signums
        self.restore = restore_at_exit
        self._old_handlers = {}
        self._fired = False

    def __enter__(self):

        def raise_handler(_signum, _stack_frame):

            # to avoid double raise
            #
            # 1) Check _fired flag
            # 2) uninstall signals
            #
            # custom _ignore_handler is used to avoid `int is not callable`
            # error with signal.SIG_IGN

            if self._fired:
                self._fired = True
                return

            for signum in self.signums:
                signal.signal(signum, RaiseOnceOnSignals._ignore_handler)

            raise StopCalculationError()

        for signum in self.signums:
            self._old_handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, raise_handler)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.restore:
            for signum in self.signums:
                signal.signal(signum, self._old_handlers[signum])

    @staticmethod
    def _ignore_handler(signum, stack_frame):
        pass
