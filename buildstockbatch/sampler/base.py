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

from buildstockbatch.utils import ContainerRuntime

logger = logging.getLogger(__name__)


class BuildStockSampler(object):
    csv_path = None

    @staticmethod
    def validate_args(project_filename, **kw):
        """Validation of args from config, passed as **kw

        :param project_filename: The path to the project configuration file
        :type project_filename: str
        :param args: arguments to pass to the sampler from the config file
        :type args: dict
        :return: True if valid
        :rtype: bool

        This is a stub. Replace it in your subclass. No need to super() anything.
        """
        return True

    def __init__(self, parent):
        """
        Create the buildstock.csv file required for batch simulations using this class.

        Multiple sampling methods are available to support local & hpc analyses, as well as to support multiple\
        sampling strategies. Currently there are separate implementations for commercial & residential stock types\
        due to unique requirements created by the commercial tsv set.

        :param parent: The BuildStockBatchBase object that owns this sampler.
        """
        self.parent = weakref.ref(parent)  # This removes circular references and allows garbage collection to work.
        if self.container_runtime in (
            ContainerRuntime.DOCKER,
            ContainerRuntime.LOCAL_OPENSTUDIO,
        ):
            self.csv_path = os.path.join(self.project_dir, "housing_characteristics", "buildstock.csv")
        elif self.container_runtime == ContainerRuntime.APPTAINER:
            self.csv_path = os.path.join(self.parent().output_dir, "housing_characteristics", "buildstock.csv")
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

    def run_sampling(self):
        """
        Execute the sampling generating the specified number of datapoints.

        Replace this in a subclass if your sampling doesn't depend on containerization.
        """
        if self.container_runtime == ContainerRuntime.DOCKER:
            return self._run_sampling_docker()
        elif self.container_runtime == ContainerRuntime.APPTAINER:
            return self._run_sampling_apptainer()
        else:
            assert self.container_runtime == ContainerRuntime.LOCAL_OPENSTUDIO
            return self._run_sampling_local_openstudio()

    def _run_sampling_docker(self):
        """
        Execute the sampling in a docker container

        Replace this in a subclass if your sampling needs docker.
        """
        raise NotImplementedError

    def _run_sampling_apptainer(self):
        """
        Execute the sampling in an apptainer container

        Replace this in a subclass if your sampling needs apptainer.
        """
        raise NotImplementedError

    def _run_sampling_local_openstudio(self):
        """
        Execute the sampling on the local openstudio instance

        Replace this in a subclass as necessary
        """
        raise NotImplementedError
