# -*- coding: utf-8 -*-

"""
buildstockbatch.sample
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging
import os
import shutil
import subprocess
import time

logger = logging.getLogger(__name__)


class BuildStockSample(object):

    def __init__(self, cfg, project_dir, buildstock_dir, n_datapoints=None):
        self.cfg = cfg
        self.n_datapoints = n_datapoints
        if self.n_datapoints is None:
            self.n_datapoints = self.cfg['baseline']['n_datapoints']
        self.project_dir = project_dir
        self.buildstock_dir = buildstock_dir

    def run_local_res_sample(self, docker_client, docker_image):
        logger.debug('Sampling, n_datapoints={}'.format(self.n_datapoints))
        tick = time.time()
        container_output = docker_client.containers.run(
            docker_image(),
            [
                'ruby',
                'resources/run_sampling.rb',
                '-p', self.cfg['project_directory'],
                '-n', str(self.n_datapoints),
                '-o', 'buildstock.csv'
            ],
            remove=True,
            volumes={
                self.buildstock_dir: {'bind': '/var/simdata/openstudio', 'mode': 'rw'}
            },
            name='buildstock_sampling'
        )
        tick = time.time() - tick
        for line in container_output.decode('utf-8').split('\n'):
            logger.debug(line)
        logger.debug('Sampling took {:.1f} seconds'.format(tick))
        destination_filename = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        if os.path.exists(destination_filename):
            os.remove(destination_filename)
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_filename
        )
        return destination_filename

    def run_peregrine_res_sample(self, singularity_image, output_dir):
        logging.debug('Sampling, n_datapoints={}'.format(self.n_datapoints))
        args = [
            'singularity',
            'exec',
            '--contain',
            '--home', self.buildstock_dir,
            singularity_image,
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(self.n_datapoints),
            '-o', 'buildstock.csv'
        ]
        subprocess.run(args, check=True, env=os.environ, cwd=output_dir)
        destination_dir = os.path.join(output_dir, 'housing_characteristics')
        if os.path.exists(destination_dir):
            shutil.rmtree(destination_dir)
        shutil.copytree(
            os.path.join(self.project_dir, 'housing_characteristics'),
            destination_dir
        )
        assert(os.path.isdir(destination_dir))
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_dir
        )
        return os.path.join(destination_dir, 'buildstock.csv')

    def run_local_com_sample(self):
        raise NotImplementedError

    def run_peregrine_com_sample(self):
        raise NotImplementedError
