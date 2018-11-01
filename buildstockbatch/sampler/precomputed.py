# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.commercial_sobol
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging
import os
import pandas as pd
import shutil

from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class PrecomputedBaseSampler(BuildStockSampler):

    def __init__(self, *args, **kwargs):
        """
        Initialize the sampler.

        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        logger.debug('args are `{}`'.format(*args))
        super().__init__(*args, **kwargs)

    def check_sampling(self, output_path, n_datapoints=None):
        """
        Check that a sampling has been precomputed in the project_dir and move to the output_dir.
        :param n_datapoints: Number of datapoints expected in the sampling
        :param output_path: Path the output csv file should occupy
        :return: Absolute path to the output buildstock.csv file
        """
        sample_number = self.cfg['baseline']['n_datapoints']
        if isinstance(n_datapoints, int):
            sample_number = n_datapoints
        logging.debug('Loading samples from the project directory')
        sample_filename = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        if not os.path.isfile(sample_filename):
            raise RuntimeError('Unable to locate precomputed sampling file at `{}`'.format(sample_filename))
        sample_df = pd.read_csv(sample_filename)
        if sample_df.shape[0] != sample_number:
            logger.warn('The number of samples requested ({}) and precomputed ({}) do not match. Continuing.'.format(
                sample_number, sample_df.shape[0])
            )
        if os.path.abspath(sample_filename) == os.path.abspath(output_path):
            return os.path.abspath(output_path)
        else:
            shutil.copy(os.path.abspath(sample_filename), os.path.abspath(output_path))
            return os.path.abspath(output_path)

    def run_sampling(self, n_datapoints=None):
        """
        Check that the sampling has been precomputed and if necessary move to the required path.

        This is a stub. It needs to be implemented in the child classes for each deployment environment.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        raise NotImplementedError


class CommercialPrecomputedSingularitySampler(PrecomputedBaseSampler):

    def __init__(self, output_dir, *args, **kwargs):
        """
        This class uses the Commercial Precomputed Sampler for Peregrine Singularity deployments

        :param output_dir: Directory in which to place buildstock.csv
        """
        logger.debug('args are `{}`'.format(*args))
        super().__init__(*args, **kwargs)
        self.output_dir = output_dir

    def run_sampling(self, n_datapoints=None):
        """
        Ensure the sampling is in place for use in Peregrine Singularity deployments

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Path to the sample CSV file
        """
        csv_path = os.path.join(self.output_dir, 'buildstock.csv')
        return self.check_sampling(csv_path, n_datapoints)


class CommercialPrecomputedDockerSampler(PrecomputedBaseSampler):

    def __init__(self, *args, **kwargs):
        """
        This class uses the Commercial Precomputed Sampler for local Docker deployments
        """
        super().__init__(*args, **kwargs)

    def run_sampling(self, n_datapoints=None):
        """
        Ensure the sampling is in place for use in local Docker deployments

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Path to the sample CSV file
        """
        csv_path = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        return self.check_sampling(csv_path, n_datapoints)

