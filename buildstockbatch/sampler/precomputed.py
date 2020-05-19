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
        super().__init__(*args, **kwargs)
        self.buildstock_csv = self.cfg['baseline']['precomputed_sample']

    def run_sampling(self, n_datapoints):
        """
        Check that the sampling has been precomputed and if necessary move to the required path.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        if self.csv_path != self.buildstock_csv:
            shutil.copy(self.buildstock_csv, self.csv_path)
        return self.csv_path


class PrecomputedSingularitySampler(PrecomputedBaseSampler):

    def __init__(self, output_dir, *args, **kwargs):
        """
        Initialize the sampler.

        :param output_dir: Simulation working directory
        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        super().__init__(*args, **kwargs)
        self.csv_path = os.path.join(output_dir, 'housing_characteristics', 'buildstock.csv')


class PrecomputedDockerSampler(PrecomputedBaseSampler):

    def __init__(self, *args, **kwargs):
        """
        Initialize the sampler.

        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        super().__init__(*args, **kwargs)
        self.csv_path = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
