# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.commercial
~~~~~~~~~~~~~~~
This object contains the commercial classes for generating OSW files from individual samples

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import datetime as dt
import json
import logging
import re
import yamale

from .base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


class CommercialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    @classmethod
    def validate(cls, cfg):
        """Validate arguments

        :param cfg: project configuration
        :type cfg: dict
        """
        schema_yml = """
        measures: list(include('measure-spec'), required=False)
        include_qaqc: bool(required=False)
        ---
        measure-spec:
            measure_dir_name: str(required=True)
            arguments: map(required=False)
        """
        workflow_generator_args = cfg['workflow_generator']['args']
        schema_yml = re.sub(r'^ {8}', '', schema_yml, flags=re.MULTILINE)
        schema = yamale.make_schema(content=schema_yml, parser='ruamel')
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser='ruamel')
        return yamale.validate(schema, data, strict=True)

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))

        workflow_args = {
            'measures': [],
            'include_qaqc': False
        }
        workflow_args.update(self.cfg['workflow_generator'].get('args', {}))

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
                    "measure_dir_name": "add_blinds_to_selected_windows",
                    "arguments": {
                        "add_blinds": True
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
            'weather_file': 'weather/empty.epw',
            'run_options': {
                'fast': True,
                'skip_expand_objects': True,
                'skip_energyplus_preprocess': True
            }
        }

        osw['steps'].extend(workflow_args['measures'])

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
            },
            {
                "measure_dir_name": "comstock_sensitivity_reports",
                "arguments": {},
                "measure_type": "ReportingMeasure"
            },
            {
                "measure_dir_name": "qoi_report",
                "arguments": {},
                "measure_type": "ReportingMeasure"
            }
        ])

        if workflow_args['include_qaqc']:
            osw['steps'].extend([
                {
                    'measure_dir_name': 'la_100_qaqc',
                    'arguments': {
                        'run_qaqc': True
                    },
                    'measure_type': 'ReportingMeasure'
                },
                {
                    'measure_dir_name': 'simulation_settings_check',
                    'arguments': {
                        'run_sim_settings_checks': True
                    },
                    'measure_type': 'ReportingMeasure'
                }
            ])

        if upgrade_idx is not None:
            measure_d = self.cfg['upgrades'][upgrade_idx]
            apply_upgrade_measure = {
                'measure_dir_name': 'ApplyUpgrade',
                'arguments': {
                    'run_measure': 1
                }
            }
            if 'upgrade_name' in measure_d:
                apply_upgrade_measure['arguments']['upgrade_name'] = measure_d['upgrade_name']
            for opt_num, option in enumerate(measure_d['options'], 1):
                apply_upgrade_measure['arguments']['option_{}'.format(opt_num)] = option['option']
                if 'lifetime' in option:
                    apply_upgrade_measure['arguments']['option_{}_lifetime'.format(opt_num)] = option['lifetime']
                if 'apply_logic' in option:
                    apply_upgrade_measure['arguments']['option_{}_apply_logic'.format(opt_num)] = \
                        self.make_apply_logic_arg(option['apply_logic'])
                for cost_num, cost in enumerate(option.get('costs', []), 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                apply_upgrade_measure['arguments']['package_apply_logic'] = \
                    self.make_apply_logic_arg(measure_d['package_apply_logic'])

            build_existing_model_idx = \
                list(map(lambda x: x['measure_dir_name'] == 'BuildExistingModel', osw['steps'])).index(True)
            osw['steps'].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        return osw
