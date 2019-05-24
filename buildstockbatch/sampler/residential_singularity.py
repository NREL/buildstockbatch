# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.residential_singularity
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging
import os
import shutil
import subprocess


from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class ResidentialSingularitySampler(BuildStockSampler):

    def __init__(self, singularity_image, output_dir, *args, **kwargs):
        """
        Initialize the sampler.

        :param singularity_image: path to the singularity image to use
        :param output_dir: Simulation working directory
        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        super().__init__(*args, **kwargs)
        self.singularity_image = singularity_image
        self.output_dir = output_dir
        self.csv_path = os.path.join(self.output_dir, 'housing_characteristics', 'buildstock.csv')

    def run_sampling(self, n_datapoints):
        """
        Run the residential sampling in a singularity container.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Absolute path to the output buildstock.csv file
        """
        logging.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        args = [
            'singularity',
            'exec',
            '--contain',
            '--home', self.buildstock_dir,
            self.singularity_image,
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(n_datapoints),
            '-o', 'buildstock.csv'
        ]
        subprocess.run(args, check=True, env=os.environ, cwd=self.output_dir)
        destination_dir = os.path.dirname(self.csv_path)
        assert(os.path.isdir(destination_dir))
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_dir
        )
        return self.csv_path
