import os
import dill
import shutil
import logging


class RemoteTask(object):
    default_save_filename = "state.dill"

    def __init__(self, name, wd, stagein_exclude_patterns=[], stageout_exclude_patterns=[]):
        self.name = name
        self.wd = wd

        self.stagein_exclude_patterns = stagein_exclude_patterns
        self.stageout_exclude_patterns = stageout_exclude_patterns

    def run(self):
        raise NotImplementedError("Implement .run() method in subclass of RemoteTask")

    def save_state(self, filename=None):
        if filename is None:
            filename = RemoteTask.default_save_filename
        filename = os.path.abspath(filename)
        out = open(filename + ".bak", 'wb')
        if out:
            logging.getLogger("remote_runner.RemoteTask").info("Writing dump to %s" % filename)
            dill.dump(self, out)
            out.close()
        else:
            logging.getLogger("remote_runner.RemoteTask").fatal("Could not open file %s to write" % filename)
        shutil.move(filename + ".bak", filename)

    @staticmethod
    def load_state(filename, cd_to_wd=True):
        """
        Returns MD stated, stored in pickle file
        :param filename: path to saved RemoteTask
        :param cd_to_wd: if true change current dir to working directory of loaded task
        :return: RemoteTask loaded from file
        :rtype: RemoteTask
        """
        import dill

        f = open(filename, "rb")
        if f:
            result = dill.load(f)
            if cd_to_wd:
                os.chdir(result.trj_home)
            return result
        else:
            raise OSError("Could not open '%s' for reading." % filename)
