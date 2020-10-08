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
import os
import weakref

from buildstockbatch import ContainerRuntime

logger = logging.getLogger(__name__)


class BuildStockSampler(object):

    csv_path = None

    def __init__(self, parent):
        """
        Create the buildstock.csv file required for batch simulations using this class.

        Multiple sampling methods are available to support local & eagle analyses, as well as to support multiple\
        sampling strategies. Currently there are separate implementations for commercial & residential stock types\
        due to unique requirements created by the commercial tsv set.

        :param parent: The BuildStockBatchBase object that owns this sampler.
        """
        self.parent = weakref.ref(parent)  # This removes circular references and allows garbage collection to work.
        if self.container_runtime == ContainerRuntime.DOCKER:
            self.csv_path = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        elif self.container_runtime == ContainerRuntime.SINGULARITY:
            self.csv_path = os.path.join(self.parent().output_dir, 'housing_characteristics', 'buildstock.csv')
        else:
            self.csv_path = None

    @property
    def cfg(self):
        return self.parent().cfg

    @property
    def buildstock_dir(self):
        return self.parent().buildstock_dir

    @property
    def project_dir(self):
        return self.parent().project_dir

    @property
    def container_runtime(self):
        return self.parent().CONTAINER_RUNTIME

    def run_sampling(self, n_datapoints):
        """
        Execute the sampling generating the specified number of datapoints.

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :type n_datapoints: int

        Replace this in a subclass if your sampling doesn't depend on containerization.
        """
        logger.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        if self.container_runtime == ContainerRuntime.DOCKER:
            return self._run_sampling_docker(n_datapoints)
        else:
            assert self.container_runtime == ContainerRuntime.SINGULARITY
            return self._run_sampling_singluarity(n_datapoints)

    def _run_sampling_docker(self, n_datapoints):
        """
        Execute the sampling in a docker container

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :type n_datapoints: int

        Replace this in a subclass if your sampling needs docker.
        """
        raise NotImplementedError

    def _run_sampling_singluarity(self, n_datapoints):
        """
        Execute the sampling in a singularity container

        :param n_datapoints: Number of datapoints to sample from the distributions.
        :type n_datapoints: int

        Replace this in a subclass if your sampling needs docker.
        """
        raise NotImplementedError
