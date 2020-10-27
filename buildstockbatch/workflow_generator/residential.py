# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import datetime as dt
import logging

from .base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


class ResidentialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))

        sample_weight = self.cfg['baseline']['n_buildings_represented'] /\
            self.cfg['baseline']['n_datapoints']
        bld_exist_model_args = {
            'building_unit_id': building_id,
            'workflow_json': 'measure-info.json',
            'sample_weight': sample_weight,
        }

        res_sim_ctl_args = {
            'timesteps_per_hr': 6,
            'begin_month': 1,
            'begin_day_of_month': 1,
            'end_month': 12,
            'end_day_of_month': 31,
            'calendar_year': 2007
        }
        res_sim_ctl_args.update(self.cfg.get('residential_simulation_controls', {}))
        bld_exist_model_args['simulation_control_timestep'] = 60 // res_sim_ctl_args['timesteps_per_hr']
        for k in ('begin_month', 'begin_day_of_month', 'end_month', 'end_day_of_month'):
            bld_exist_model_args[f'simulation_control_run_period_{k}'] = res_sim_ctl_args[k]

        if 'measures_to_ignore' in self.cfg['baseline']:
            bld_exist_model_args['measures_to_ignore'] = '|'.join(self.cfg['baseline']['measures_to_ignore'])

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
        }

        osw['steps'].extend(self.cfg['baseline'].get('measures', []))

        sim_output_args = {
            'timeseries_frequency': 'hourly',
            'include_timeseries_zone_temperatures': False,
            'include_timeseries_fuel_consumptions': False,
            'include_timeseries_end_use_consumptions': False,
            'include_timeseries_hot_water_uses': False,
            'include_timeseries_total_loads': False,
            'include_timeseries_component_loads': False
        }
        sim_output_args.update(self.cfg.get('simulation_output', {}))

        osw['steps'].extend([
            {
                'measure_dir_name': 'SimulationOutputReport',
                'arguments': sim_output_args
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

        if 'reporting_measures' in self.cfg:
            for measure_dir_name in self.cfg['reporting_measures']:
                reporting_measure = {
                    'measure_dir_name': measure_dir_name,
                    'arguments': {}
                }
                osw['steps'].insert(-1, reporting_measure)  # right before ServerDirectoryCleanup

        return osw
