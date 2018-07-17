import yaml
import os
import logging


class BuildStockBatchBase(object):

    def __init__(self, project_filename):
        self.project_filename = os.path.abspath(project_filename)
        with open(self.project_filename, 'r') as f:
            self.cfg = yaml.load(f)

    @property
    def buildstock_dir(self):
        if os.path.isabs(self.cfg['buildstock_directory']):
            d = os.path.abspath(self.cfg['buildstock_directory'])
        else:
            d = os.path.abspath(
                os.path.join(
                    os.path.dirname(self.project_filename),
                    self.cfg['buildstock_directory']
                )
            )
        # logging.debug('buildstock_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    @property
    def project_dir(self):
        d = os.path.abspath(
            os.path.join(self.buildstock_dir, self.cfg['project_directory'])
        )
        # logging.debug('project_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    def run_sampling(self):
        raise NotImplementedError

    def run_batch(self):
        raise NotImplementedError
