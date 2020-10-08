"""
buildstockbatch.sampler.residential_quota
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2020 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import docker
import logging
import os
import shutil
import subprocess
import sys
import time

from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class ResidentialQuotaSampler(BuildStockSampler):

    def _run_sampling_docker(self, n_datapoints):
        docker_client = docker.DockerClient.from_env()
        tick = time.time()
        extra_kws = {}
        if sys.platform.startswith('linux'):
            extra_kws['user'] = f'{os.getuid()}:{os.getgid()}'
        container_output = docker_client.containers.run(
            self.parent().docker_image,
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

    def _run_sampling_singularity(self, n_datapoints):
        args = [
            'singularity',
            'exec',
            '--contain',
            '--home', '{}:/buildstock'.format(self.buildstock_dir),
            '--bind', '{}:/outbind'.format(os.path.dirname(self.csv_path)),
            self.parent().singularity_image,
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(n_datapoints),
            '-o', '../../outbind/{}'.format(os.path.basename(self.csv_path))
        ]
        logger.debug(f"Starting singularity sampling with command: {' '.join(args)}")
        subprocess.run(args, check=True, env=os.environ, cwd=self.parent().output_dir)
        logger.debug("Singularity sampling completed.")
        return self.csv_path
