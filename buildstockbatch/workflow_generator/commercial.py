# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.commercial
~~~~~~~~~~~~~~~
This object contains the commercial classes for generating OSW files from individual samples

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import logging

from .base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


class CommercialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))
        raise NotImplementedError
