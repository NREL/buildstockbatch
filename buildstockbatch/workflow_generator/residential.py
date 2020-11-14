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
import json
import logging
import re
import yamale

from .base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


class ResidentialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    @classmethod
    def validate(cls, workflow_generator_args):
        """Validate arguments

        :param workflow_generator_args: Arguments passed to the workflow generator in the yaml
        :type workflow_generator_args: dict
        """
        schema_yml = """
        measures_to_ignore: list(str(), required=False)
        residential_simulation_controls: map(required=False)
        measures: list(include('measure-spec'), required=False)
        simulation_output: map(required=False)
        timeseries_csv_export: map(required=False)
        reporting_measures: list(str(), required=False)
        ---
        measure-spec:
            measure_dir_name: str(required=True)
            arguments: map(required=False)
        """
        schema_yml = '\n'.join([re.sub(r'^ {8}', '', x) for x in schema_yml.strip().split('\n')])
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
        # Default argument values
        workflow_args = {
            'residential_simulation_controls': {},
            'measures': [],
            'simulation_output': {}
        }
        workflow_args.update(self.cfg['workflow_generator'].get('args', {}))

        logger.debug('Generating OSW, sim_id={}'.format(sim_id))

        res_sim_ctl_args = {
            'timesteps_per_hr': 6,
            'begin_month': 1,
            'begin_day_of_month': 1,
            'end_month': 12,
            'end_day_of_month': 31,
            'calendar_year': 2007
        }
        res_sim_ctl_args.update(workflow_args['residential_simulation_controls'])

        # FIXME: The sample weight will likely be wrong for a downselect.
        bld_exist_model_args = {
            'building_id': building_id,
            'workflow_json': 'measure-info.json',
            'number_of_buildings_represented': self.cfg['baseline']['n_buildings_represented']
        }
        if 'measures_to_ignore' in workflow_args:
            bld_exist_model_args['measures_to_ignore'] = '|'.join(workflow_args['measures_to_ignore'])

        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'ResidentialSimulationControls',
                    'arguments': res_sim_ctl_args,
                },
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': bld_exist_model_args
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
        }

        osw['steps'].extend(workflow_args['measures'])

        osw['steps'].extend([
            {
                'measure_dir_name': 'SimulationOutputReport',
                'arguments': workflow_args['simulation_output']
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

        if 'timeseries_csv_export' in workflow_args:
            timeseries_csv_export_args = {
                'reporting_frequency': 'Hourly',
                'include_enduse_subcategories': False,
                'output_variables': ''
            }
            timeseries_csv_export_args.update(workflow_args['timeseries_csv_export'])
            timeseries_measure = {
                'measure_dir_name': 'TimeseriesCSVExport',
                'arguments': timeseries_csv_export_args
            }
            osw['steps'].insert(-1, timeseries_measure)  # right before ServerDirectoryCleanup

        if 'reporting_measures' in workflow_args:
            for measure_dir_name in workflow_args['reporting_measures']:
                reporting_measure = {
                    'measure_dir_name': measure_dir_name,
                    'arguments': {}
                }
                osw['steps'].insert(-1, reporting_measure)  # right before ServerDirectoryCleanup

        return osw
