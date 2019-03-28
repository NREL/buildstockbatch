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
import datetime as dt

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
        osw = {
            'id': sim_id,
            'steps': [
                {
                    "measure_dir_name": "BuildExistingModel",
                    "arguments": {
                        "number_of_buildings_represented": 1,
                        "building_id": int(building_id)
                    },
                    "measure_type": "ModelMeasure"
                },
                {
                    "measure_dir_name": "set_space_type_load_subcategories",
                    "arguments": {},
                    "measure_type": "ModelMeasure"
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
            'seed_file': 'seeds/empty.osm',
            'weather_file': 'weather/CA_LOS-ANGELES-DOWNTOWN-USC_722874S_12.epw'
        }

        osw['steps'].extend(self.cfg['baseline'].get('measures', []))

        osw['steps'].extend([
            {
                "measure_dir_name": "SimulationOutputReport",
                "arguments": {},
                "measure_type": "ReportingMeasure"
            },
            {
                "measure_dir_name": "f8e23017-894d-4bdf-977f-37e3961e6f42",
                "arguments": {
                    "building_summary_section": True,
                    "annual_overview_section": True,
                    "monthly_overview_section": True,
                    "utility_bills_rates_section": True,
                    "envelope_section_section": True,
                    "space_type_breakdown_section": True,
                    "space_type_details_section": True,
                    "interior_lighting_section": True,
                    "plug_loads_section": True,
                    "exterior_light_section": True,
                    "water_use_section": True,
                    "hvac_load_profile": True,
                    "zone_condition_section": True,
                    "zone_summary_section": True,
                    "zone_equipment_detail_section": True,
                    "air_loops_detail_section": True,
                    "plant_loops_detail_section": True,
                    "outdoor_air_section": True,
                    "cost_summary_section": True,
                    "source_energy_section": True,
                    "schedules_overview_section": True
                },
                "measure_type": "ReportingMeasure"
            },
            {
                "measure_dir_name": "TimeseriesCSVExport",
                "arguments": {
                    "reporting_frequency": "Timestep",
                    "inc_output_variables": False
                },
                "measure_type": "ReportingMeasure"
            }
            # {
            #     "measure_dir_name": "la_100_qaqc",
            #     "arguments": {
            #         "run_qaqc": True
            #     },
            #     "measure_type": "ReportingMeasure"
            # },
            # {
            #     "measure_dir_name": "simulation_settings_check",
            #     "arguments": {
            #         "run_sim_settings_checks": True
            #     },
            #     "measure_type": "ReportingMeasure"
            # }
        ])

        return osw
