# -*- coding: utf-8 -*-
"""
buildstockbatch.sampler.base
~~~~~~~~~~~~~~~
This object contains the base class for the samplers.

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging

logger = logging.getLogger(__name__)


class BuildStockSampler(object):

    csv_path = None

    def __init__(self, cfg, buildstock_dir, project_dir):
        """
        Create the buildstock.csv file required for batch simulations using this class.

        Multiple sampling methods are available to support local & peregrine analyses, as well as to support multiple\
        sampling strategies. Currently there are separate implementations for commercial & residential stock types\
        due to unique requirements created by the commercial tsv set.

        :param cfg: YAML configuration specified by the user for the analysis
        :param buildstock_dir: The location of the OpenStudio-BuildStock repo
        :param project_dir: The project directory within the OpenStudio-BuildStock repo
        """
        self.cfg = cfg
        self.buildstock_dir = buildstock_dir
        self.project_dir = project_dir

    def run_sampling(self, n_datapoints=None):
        """
        Execute the sampling generating the specified number of datapoints.

        This is a stub. It needs to be implemented in the child classes.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        """
        raise NotImplementedError
