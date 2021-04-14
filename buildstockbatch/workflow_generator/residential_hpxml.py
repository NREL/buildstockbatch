# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential_hpxml
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Joe Robertson
:copyright: (c) 2021 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import datetime as dt
import json
import logging
import re
import yamale

from .base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


class ResidentialHpxmlWorkflowGenerator(WorkflowGeneratorBase):

    @classmethod
    def validate(cls, cfg):
        """Validate arguments

        :param cfg: project configuration
        :type cfg: dict
        """
        schema_yml = """
        build_existing_model: map(required=False)
        simulation_output_report: map(required=False)
        ---
        measure-spec:
            measure_dir_name: str(required=True)
            arguments: map(required=False)
        """
        workflow_generator_args = cfg['workflow_generator']['args']
        schema_yml = re.sub(r'^ {8}', '', schema_yml, flags=re.MULTILINE)
        schema = yamale.make_schema(content=schema_yml, parser='ruamel')
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser='ruamel')
        yamale.validate(schema, data, strict=True)
        return True

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        # Default argument values
        workflow_args = {
            'build_existing_model': {},
            'simulation_output_report': {},
        }
        workflow_args.update(self.cfg['workflow_generator'].get('args', {}))

        logger.debug('Generating OSW, sim_id={}'.format(sim_id))

        sim_ctl_run_prd_args = {
            'simulation_control_run_period_begin_month': 1,
            'simulation_control_run_period_begin_day_of_month': 1,
            'simulation_control_run_period_end_month': 12,
            'simulation_control_run_period_end_day_of_month': 31,
            'simulation_control_run_period_calendar_year': 2007,
        }

        bld_exist_model_args = {
            'building_id': building_id,
            'workflow_json': 'measure-info.json',
            'sample_weight': self.n_datapoints / self.cfg['baseline']['n_buildings_represented'],
        }
        bld_exist_model_args.update(sim_ctl_run_prd_args)
        bld_exist_model_args.update(workflow_args['build_existing_model'])

        sim_out_rep_args = {
            'timeseries_frequency': True,
            'include_timeseries_fuel_consumptions': False,
            'include_timeseries_end_use_consumptions': False,
            'include_timeseries_hot_water_uses': False,
            'include_timeseries_total_loads': False,
            'include_timeseries_component_loads': False,
            'include_timeseries_unmet_loads': False,
            'include_timeseries_zone_temperatures': False,
            'include_timeseries_airflows': False,
            'include_timeseries_weather': False,
        }
        sim_out_rep_args.update(workflow_args['simulation_output_report'])

        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': bld_exist_model_args
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures',
                'resources/hpxml-measures'
            ],
            'run_options': {
                'fast': True,
                'skip_expand_objects': True,
                'skip_energyplus_preprocess': True
            }
        }

        osw['steps'].extend([
            {
                'measure_dir_name': 'SimulationOutputReport',
                'arguments': workflow_args['simulation_output_report']
            },
            {
                'measure_dir_name': 'UpgradeCosts',
                'arguments': {}
            },
            {
                'measure_dir_name': 'ServerDirectoryCleanup',
                'arguments': {}
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
                [x['measure_dir_name'] == 'BuildExistingModel' for x in osw['steps']].index(True)
            osw['steps'].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        return osw
