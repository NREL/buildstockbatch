# -*- coding: utf-8 -*-

"""
buildstockbatch.osw
~~~~~~~~~~~~~~~
This object contains the code required for generating OSW files from individual samples

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from copy import deepcopy
import datetime as dt


class BuildStockOsw(object):

    def __init__(self, cfg):
        self.cfg = cfg

    def create_res_osw(self, sim_id, i, upgrade_idx):
        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': {
                        'building_id': i,
                        'workflow_json': 'measure-info.json',
                        'sample_weight': self.cfg['baseline']['n_buildings_represented'] /
                                         self.cfg['baseline']['n_datapoints']
                    }
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
            'seed_file': 'seeds/EmptySeedModel.osm',
            'weather_file': 'weather/Placeholder.epw'
        }

        osw['steps'].extend(self.cfg['baseline'].get('measures', []))

        osw['steps'].extend([
            {
                'measure_dir_name': 'BuildingCharacteristicsReport',
                'arguments': {}
            },
            {
                'measure_dir_name': 'SimulationOutputReport',
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
                    'upgrade_name': measure_d['upgrade_name'],
                    'run_measure': 1
                }
            }
            for opt_num, option in enumerate(measure_d['options'], 1):
                apply_upgrade_measure['arguments']['option_{}'.format(opt_num)] = option['option']
                if 'lifetime' in option:
                    apply_upgrade_measure['arguments']['option_{}_lifetime'.format(opt_num)] = option['lifetime']
                if 'apply_logic' in option:
                    apply_upgrade_measure['arguments']['option_{}_apply_logic'.format(opt_num)] = \
                        self.make_apply_logic_arg(option['apply_logic'])
                for cost_num, cost in enumerate(option['costs'], 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                apply_upgrade_measure['package_apply_logic'] = self.make_apply_logic_arg(
                    measure_d['package_apply_logic'])

            osw['steps'].insert(1, apply_upgrade_measure)

        if 'timeseries_csv_export' in self.cfg:
            timeseries_measure = {
                'measure_dir_name': 'TimeseriesCSVExport',
                'arguments': deepcopy(self.cfg['timeseries_csv_export'])
            }
            timeseries_measure['arguments']['output_variables'] = \
                ','.join(self.cfg['timeseries_csv_export']['output_variables'])
            osw['steps'].insert(-1, timeseries_measure)

        return osw

    def make_apply_logic_arg(self, logic):
        if isinstance(logic, dict):
            assert (len(logic) == 1)
            key = list(logic.keys())[0]
            if key == 'and':
                return '(' + '&&'.join(map(self.make_apply_logic_arg, logic[key])) + ')'
            elif key == 'or':
                return '(' + '||'.join(map(self.make_apply_logic_arg, logic[key])) + ')'
            elif key == 'not':
                return '!' + self.make_apply_logic_arg(logic[key])
        elif isinstance(logic, list):
            return '(' + '&&'.join(map(self.make_apply_logic_arg, logic)) + ')'
        elif isinstance(logic, str):
            return logic

    def create_com_osw(self):
        raise NotImplementedError
