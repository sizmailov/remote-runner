from __future__ import absolute_import
from .RemoteRunner import RemoteRunner
from .RemoteRunner import join_thread
import threading
from threading import Lock
import logging

class RemotePool(object):

    def __init__(self, runners_init_parameters):
        self.runners_init_parameters = runners_init_parameters
        self.remotes = []
        self.threads = []
        self.exiting = threading.Event()
        self.exiting.clear()
        self.l = Lock()

    def run(self, sim_prm):
        import time
        self.taken_tasks = 0

        def process_queue(i):

            with self.l:
                self.logger().info("Starting processing")

            while True:
                with self.l:
                    if self.exiting.is_set():
                        return
                    time.sleep(1.0)
                    remote = RemoteRunner(**self.runners_init_parameters[i])
                    self.remotes.append(remote)
                    if self.taken_tasks >= len(sim_prm):
                        break
                    tid = self.taken_tasks
                    self.taken_tasks += 1

                    self.logger().info("Processing task", tid, threading.current_thread().name)

                remote.run(**sim_prm[tid])

                with self.l:
                    self.remotes.remove(remote)

        self.threads = [threading.Thread(target=process_queue, args=(i,)) for i in range(len(self.runners_init_parameters))]

        for t in self.threads:
            t.start()

        for t in self.threads:
            join_thread(t)

    def close(self):

        self.exiting.set()
        with self.l:
            self.logger().info("CLOSING")
            try:
                self.logger().info("EXITING")
                for r in self.remotes:
                    print(r, r.exiting)
                    r.exiting.set()
            except:
                self.logger().error("NOEXCEPT!!!!")
                pass

        self.logger().info("JOINING > ")
        for t in self.threads:
            self.logger().info("join ... ")
            join_thread(t)
        self.logger().info("JOINING < ")

    @staticmethod
    def logger():
        return logging.getLogger("remote_runner.RemotePool")


