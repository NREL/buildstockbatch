# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.residential_docker
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import docker
import logging
import os
import shutil
import sys
import time

from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class ResidentialDockerSampler(BuildStockSampler):

    def __init__(self, docker_image, *args, **kwargs):
        """
        Initialize the sampler.

        :param docker_image: the docker image to use (i.e. nrel/openstudio:2.7.0)
        :return: Absolute path to the output buildstock.csv file
        """
        super().__init__(*args, **kwargs)
        self.docker_image = docker_image
        self.csv_path = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')

    def run_sampling(self, n_datapoints=None):
        """
        Run the residential sampling in a docker container.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        if n_datapoints is None:
            raise AttributeError('n_datapoints passed to sampler is None. Please specify for this sampler')
        docker_client = docker.DockerClient.from_env()
        logger.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        tick = time.time()
        extra_kws = {}
        if sys.platform.startswith('linux'):
            extra_kws['user'] = f'{os.getuid()}:{os.getgid()}'
        container_output = docker_client.containers.run(
            self.docker_image,
            [
                'ruby',
                'resources/run_sampling.rb',
                '-p', self.cfg['project_directory'],
                '-n', str(n_datapoints),
                '-o', 'buildstock.csv'
            ],
            remove=True,
            volumes={
                self.buildstock_dir: {'bind': '/var/simdata/openstudio', 'mode': 'rw'}
            },
            name='buildstock_sampling',
            **extra_kws
        )
        tick = time.time() - tick
        for line in container_output.decode('utf-8').split('\n'):
            logger.debug(line)
        logger.debug('Sampling took {:.1f} seconds'.format(tick))
        destination_filename = self.csv_path
        if os.path.exists(destination_filename):
            os.remove(destination_filename)
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_filename
        )
        return destination_filename
