# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential_hpxml_hes
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Joe Robertson
:copyright: (c) 2021 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import os
import yamale
from .residential_hpxml import ResidentialHpxmlWorkflowGenerator  # noqa F041


class ResidentialHpxmlHesWorkflowGenerator(ResidentialHpxmlWorkflowGenerator):

    @property
    def os_hescore_dir(self):
        return self.cfg["workflow_generator"]["args"]["build_existing_model"]["os_hescore_directory"]

    def create_osw(self, sim_id, building_id, upgrade_idx):
        osw = super().create_osw(sim_id, building_id, upgrade_idx)
        if "os_hescore_directory" in osw["steps"][0]["arguments"]:
            osw["steps"][0]["arguments"]["os_hescore_directory"] = self.os_hescore_dir

        # Add measure path for reporting measure
        osw["measure_paths"].insert(0, os.path.join(self.os_hescore_dir, "hpxml-measures"))
        return osw

    @classmethod
    def get_yml_schema(cls):
        schema = super().get_yml_schema()

        # Require os_hescore_directory argument
        string_validator = yamale.validators.String(required=True)
        schema.includes["build-existing-model-spec"].dict["os_hescore_directory"] = string_validator
        return schema
