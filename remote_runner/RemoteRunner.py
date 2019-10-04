from __future__ import print_function
import os
import sys
import subprocess
import threading
import paramiko
import hashlib
import glob
import logging

from .RemoteTask import RemoteTask


class RemoteRunner(object):
    ssh_config_file = "~/.ssh/config"
    logger = logging.getLogger("remote_runner.RemoteRunner")

    def __init__(self, host, remote_tmp_root="~/remote_runner_tmp", remote_environment_setup=[]):
        self.host = host
        self.remote_tmp_root = remote_tmp_root
        self.remote_environment_setup = remote_environment_setup
        self.connect()
        self.exiting = threading.Event()
        self.thread_exception = None
        self.rsync_fails_in_row = 0

    def connect(self):
        client = paramiko.SSHClient()
        client._policy = paramiko.WarningPolicy()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(**self.get_host_config())
        self.client = client

    def get_host_config(self):
        ssh_config = paramiko.SSHConfig()
        user_config_file = os.path.expanduser(self.ssh_config_file)

        if os.path.exists(user_config_file):
            with open(user_config_file) as f:
                ssh_config.parse(f)

        cfg = {'hostname': self.host}
        user_config = ssh_config.lookup(cfg['hostname'])
        if 'hostname' in user_config:
            cfg['hostname'] = user_config['hostname']
        if 'user' in user_config:
            cfg['username'] = user_config['user']
        for k in ('port',):
            if k in user_config:
                cfg[k] = int(user_config[k])
        if 'proxycommand' in user_config:
            cfg['sock'] = paramiko.ProxyCommand(user_config['proxycommand'])

        return cfg

    def disconnect(self):
        self.client.close()

    def stagein(self, task):
        """
        :param (RemoteTask) task: task
        :return: None
        """
        self.logger.info("RemoteRunner[%s]: stagein" % task.name)
        if self.remote_tmp_root is None: return
        self.logger.info("Preparing [%s] to run" % self.host)

        local_wd = os.path.abspath(task.wd)
        remote_wd = self.remote_tmp_wd(local_wd)
        self.exec_cmd("mkdir -p %s" % remote_wd)

        ecode = subprocess.call(["rsync", "-av"]
                                + ["--exclude=%s" % pattern for pattern in task.stagein_exclude_patterns]
                                + [local_wd + "/"]
                                + [self.host + ":" + remote_wd])
        assert (ecode == 0)

    def remote_tmp_wd(self, directory):
        if self.remote_tmp_root is None: return directory
        m = hashlib.md5()
        m.update(os.path.abspath(directory).encode('utf-8'))
        remote_wd = self.remote_tmp_root + "/" + m.hexdigest()
        return remote_wd

    def run(self, state, rsync_period):
        state_abs_path = os.path.abspath(state)
        task = RemoteTask.load_state(state, cd_to_wd=False)
        rel_state_path = os.path.relpath(state_abs_path, task.wd)
        try:
            self.stagein(task)
            try:
                self.remote_run_with_rsync(task, state=rel_state_path, rsync_period=rsync_period)
                if self.thread_exception is not None:
                    raise self.thread_exception
            except:
                self.disconnect()
                self.connect()
                self.stageout(task)
                self.client.close()
                raise
        except KeyboardInterrupt as e:
            self.logger.debug("RemoteRunner[%s]: keyboard interrupt" % task.name)
            self.report_error(task, "interrupted")
            raise
        except:
            self.logger.debug("RemoteRunner[%s]: unknown error" % task.name)
            self.report_error(task, "unknown error")
            raise

    def remote_run_with_rsync(self, task, state, rsync_period):
        """
        :param (RemoteTask) task:
        :param state:
        :param rsync_period:
        :return:
        """
        self.logger.info("RemoteRunner[%s]: remote run" % task.name)
        import threading

        remote_run_complete = threading.Event()
        remote_run_complete.clear()

        def call_run_simple():
            import time
            try:
                cmd = "\n".join(
                    [
                        "source /etc/profile",
                        "source ~/.profile"
                    ] +
                    self.remote_environment_setup
                    +
                    [
                        "cd %s" % self.remote_tmp_wd(task.wd),
                        "nohup python -c 'from remote_runner import RemoteTask;task=RemoteTask.load_state(\"{state}\",cd_to_wd=False); task.run();' > stdout 2> stderr &".format(
                            state=state),
                        "echo $!",
                    ])

                exit_code, out, err = self.exec_cmd(cmd)
                remote_pid = int(out.strip().split(b"\n")[-1])

                self.logger.debug("RemoteRunner[%s]: remote pid %d" % (task.wd, remote_pid))

                kill_tries = 0
                while True:
                    c, o, e = self.exec_cmd("ps -p %d -o comm=" % remote_pid, tee_to_host=False)
                    if c != 0:
                        self.logger.debug("RemoteRunner[%s]: remote run complete " % task.wd)
                        remote_run_complete.set()
                        break
                    else:
                        pass
                        # RemoteRunner.log_debug("RemoteRunner[%s]: remote ps:" % md._name + o)

                    if self.exiting.is_set():
                        self.logger.debug("RemoteRunner[%s]: exiting is set " % task.name)
                        self.thread_exception = KeyboardInterrupt("Interrupted")
                        c, o, e = self.exec_cmd("kill -15 %d" % (remote_pid))
                        self.logger.debug("RemoteRunner[%s]: killing -15 remote job: %s, %s" % (task.name, o, e))
                        kill_tries += 1
                        if kill_tries > 3:
                            c, o, e = self.exec_cmd("kill -15 %d" % (remote_pid))
                            self.logger.debug(
                                "RemoteRunner[%s]: killing -9 remote job: %s, %s" % (task.name, o, e))

                    time.sleep(3.0)

                if exit_code != 0:
                    self.logger.debug("RemoteRunner[%s]: remote exit code %d" % (task.name, exit_code))
                    self.report_error(task, "run error", body="Output:\n%s\n\nStderr:\n%s\n\n" % (out, err))
                    self.thread_exception = Exception("Return status is %d" % exit_code)
            except Exception as e:
                print(e)
                self.thread_exception = e
            finally:
                remote_run_complete.set()

        call_run = call_run_simple

        t = threading.Thread(target=call_run)
        t.start()

        self.logger.debug("RemoteRunner[%s]: thread spawned" % (task.name))

        if self.remote_tmp_root is not None:
            while not remote_run_complete.is_set() and not self.exiting.is_set():
                remote_run_complete.wait(timeout=rsync_period)
                self._rsync_trj_home_with_remote(task)

        self.logger.debug("RemoteRunner[%s]: joining thread" % (task.name))

        join_thread(t)

        self.logger.debug("RemoteRunner[%s]: thread joined" % (task.name))

    def stageout(self, task):
        """
        :param (RemoteTask) task:
        :return: None
        """
        self.logger.debug("RemoteRunner[%s]: stageout" % task.name)
        if self.remote_tmp_root is None: return
        self._rsync_trj_home_with_remote(task)
        self.exec_cmd("rm -r %s" % self.remote_tmp_wd(task.wd))

    def _rsync_trj_home_with_remote(self, task):
        """
        :param (RemoteTask) task:
        :return: None
        """

        trj_home = os.path.abspath(task.wd)
        remote_trj_home = self.remote_tmp_wd(task.wd)

        exclude_list = task.stageout_exclude_patterns

        rsync_cmd = ["rsync", "-a"] + exclude_list + [self.host + ":" + remote_trj_home + "/", trj_home + "/"]
        self.logger.debug("RemoteRunner[%s]: rsync command: %s" % (task.name, rsync_cmd))

        ecode = subprocess.call(rsync_cmd)
        self.logger.debug("RemoteRunner[%s]: rsync exit code: %d" % (task.name, ecode))

        if ecode != 0:
            self.rsync_fails_in_row += 1
            if self.rsync_fails_in_row > 5:
                self.report_error(task, "rsync fails 5 times in a row")
        else:
            self.rsync_fails_in_row = 0

    def exec_cmd(self, cmd, stdin=None, tee_to_host=True):
        import time
        channel = self.client.get_transport().open_session()
        channel.exec_command(cmd)
        if stdin is not None:
            inp = channel.makefile('wb', -1)
            inp.write(stdin)
            inp.flush()
            inp.close()
            channel.shutdown_write()

        out = b''
        err = b''

        while not channel.exit_status_ready():  # monitoring process
            while channel.recv_ready():
                out += channel.recv(80)
            while channel.recv_stderr_ready():
                err += channel.recv_stderr(80)
            time.sleep(1)

        while channel.recv_ready():
            out += channel.recv(80)
        while channel.recv_stderr_ready():
            err += channel.recv_stderr(80)

        if tee_to_host:
            sys.stderr.write(err.decode('utf-8'))
            sys.stdout.write(out.decode('utf-8'))

        return channel.recv_exit_status(), out, err

    def report_error(self, task, message, body=""):
        """
        
        :param (RemoteTask) task: 
        :param message:
        :param body: 
        :return: 
        """

        import socket
        try:
            import postman
            subject = "Error: %s (%s)" % (task.name, message)
            text = "Run fails for some reason, for details, please see\n"
            text += socket.gethostname() + ":" + task.wd
            text += "\n" + "=" * 80 + "\n\n"
            text += body
            postman.send_mail(subject, text)
        except:
            print("Can't send mail", file=sys.stderr)


#
# see for details http://stackoverflow.com/q/631441/1741477
#
def join_thread(thread):
    while thread.is_alive():
        thread.join(timeout=1000)
    else:
        thread.join()
