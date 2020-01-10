# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.precomputed
~~~~~~~~~~~~~~~
This object contains the code required for ingesting an already existing buildstock.csv file

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
        sample_number = self.cfg['baseline'].get('n_datapoints', 350000)
        if isinstance(n_datapoints, int):
            sample_number = n_datapoints
        logging.debug('Loading samples from the project directory')
        sample_filename = self.cfg['baseline'].get('precomputed_sample', None)
        if not sample_filename:
            sample_filename = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        if not os.path.isfile(sample_filename):
            raise RuntimeError('Unable to locate precomputed sampling file at `{}`'.format(sample_filename))
        sample_df = pd.read_csv(sample_filename)
        if sample_df.shape[0] != sample_number:
            raise RuntimeError(
                'A buildstock_csv was provided, so n_datapoints for sampling should not be provided or should be equal '
                'to the number of rows in the buildstock.csv file. Remove or comment out baseline->n_datapoints from '
                'your project file.'
            )
        if os.path.abspath(sample_filename) == os.path.abspath(output_path):
            return os.path.abspath(output_path)
        else:
            shutil.copyfile(os.path.abspath(sample_filename), os.path.abspath(output_path))
            return os.path.abspath(output_path)

    def run_sampling(self, n_datapoints=None):
        """
        Check that the sampling has been precomputed and if necessary move to the required path.

        This is a stub. It needs to be implemented in the child classes for each deployment environment.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        raise NotImplementedError


class PrecomputedSingularitySampler(PrecomputedBaseSampler):

    def __init__(self, output_dir, *args, **kwargs):
        """
        This class uses the Commercial Precomputed Sampler for Peregrine Singularity deployments

        :param output_dir: Directory in which to place buildstock.csv
        """
        logger.debug('args are `{}`'.format(*args))
        super().__init__(*args, **kwargs)
        if 'downselect' in self.cfg:
            raise RuntimeError(
                'A buildstock_csv was provided, which isn\'t compatible with downselecting.'
                'Remove or comment out the downselect key from your project file.'
            )
        self.output_dir = output_dir
        self.csv_path = os.path.join(self.output_dir, 'housing_characteristics', 'buildstock.csv')

    def run_sampling(self, n_datapoints=None):
        """
        Ensure the sampling is in place for use in Peregrine Singularity deployments

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :return: Path to the sample CSV file
        """
        csv_path = os.path.join(self.output_dir, 'buildstock.csv')
        if self.cfg.get('baseline', False):
            if self.cfg['baseline'].get('precomputed_sample', False):
                buildstock_path = self.cfg['baseline']['precomputed_sample']
                if not os.path.isfile(buildstock_path):
                    raise RuntimeError('Cannot find buildstock file {}'.format(buildstock_path))
                shutil.copy2(buildstock_path, self.output_dir)
                shutil.move(os.path.join(self.output_dir, os.path.basename(buildstock_path)), csv_path)
                shutil.copy2(csv_path, os.path.join(self.output_dir, 'housing_characteristics', 'buildstock.csv'))
        return self.check_sampling(csv_path, n_datapoints)


class PrecomputedDockerSampler(PrecomputedBaseSampler):

    def __init__(self, output_dir, *args, **kwargs):
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
        if self.cfg.get('baseline', False):
            if self.cfg['baseline'].get('precomputed_sample', False):
                buildstock_path = self.cfg['baseline']['precomputed_sample']
                if not os.path.isfile(buildstock_path):
                    raise RuntimeError('Cannot find buildstock file {}'.format(buildstock_path))
                housing_chars_dir = os.path.join(self.project_dir, 'housing_characteristics')
                housing_chars_buildstock_path = os.path.join(housing_chars_dir, os.path.basename(buildstock_path))
                # Copy the arbitrarily named buildstock file into the housing_characteristics directory
                # (unless it is already in that directory)
                if os.path.abspath(buildstock_path) != os.path.abspath(housing_chars_buildstock_path):
                    logger.debug(f"Copying {buildstock_path} to \n {housing_chars_buildstock_path}")
                    shutil.copy2(buildstock_path, housing_chars_buildstock_path)
                # Copy the arbitrarily named buildstock file to 'buildstock.csv'
                # (unless it is already named 'buildstock.csv')
                if os.path.abspath(housing_chars_buildstock_path) != os.path.abspath(csv_path):
                    logger.debug(f"Copying {housing_chars_buildstock_path} to \n {csv_path}")
                    shutil.copy2(housing_chars_buildstock_path, csv_path)
                # Copy the 'buildstock.csv' file back to the project root
                project_root_buildstock_path = os.path.join(self.project_dir, 'buildstock.csv')
                if os.path.abspath(csv_path) != os.path.abspath(project_root_buildstock_path):
                    logger.debug(f"Copying {housing_chars_buildstock_path} to \n {csv_path}")
                    shutil.copy2(csv_path, project_root_buildstock_path)
        return self.check_sampling(csv_path, n_datapoints)
