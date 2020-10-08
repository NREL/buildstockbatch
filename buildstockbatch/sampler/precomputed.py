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
import shutil

from .base import BuildStockSampler

logger = logging.getLogger(__name__)


class PrecomputedSampler(BuildStockSampler):

    def __init__(self, parent):
        super().__init__(parent)
        self.buildstock_csv = self.cfg['baseline']['precomputed_sample']

    def run_sampling(self, n_datapoints):
        """
        Check that the sampling has been precomputed and if necessary move to the required path.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        if self.csv_path != self.buildstock_csv:
            shutil.copy(self.buildstock_csv, self.csv_path)
        return self.csv_path
