# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential_hpxml_hes
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Joe Robertson
:copyright: (c) 2021 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from .residential_hpxml import ResidentialHpxmlWorkflowGenerator  # noqa F041


class ResidentialHpxmlHesWorkflowGenerator(ResidentialHpxmlWorkflowGenerator):

    def create_osw(self, sim_id, building_id, upgrade_idx):
        osw = super().create_osw(sim_id, building_id, upgrade_idx)
        if 'os_hescore_directory' in osw['steps'][0]['arguments']:
            osw['steps'][0]['arguments']['os_hescore_directory'] = '../../OpenStudio-HEScore'

        # Add measure path for reporting measure
        osw['measure_paths'].insert(0, 'OpenStudio-HEScore/hpxml-measures')

        return osw
