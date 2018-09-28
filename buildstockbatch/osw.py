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
import logging
from warnings import warn

logger = logging.getLogger(__name__)


class BuildStockOsw(object):

    OSW_DEFAULTS = {
        'commercial': {
            'osw_template': 'default',
            'ts_report_freq': 4,
            'n_buildings_represented': 2000000
        },
        'residential': {
            'osw_template': 'default',
            'n_buildings_represented': 25000000
        }
    }

    def __init__(self, cfg, n_datapoints=None):
        """
        Create the input OSW file required for executing a single simulation using this class.\
        This class can produce varying OSW inputs based on input defined by the user. This allows for residential and\
        commercial OSW templates to drift slightly based on individual requirements, and provides extensibility in case\
        custom OSW templates are required in the future for special purposes.
        :param cfg: YAML configuration specified by the user for the analysis
        :param n_datapoints: This parameter seems to be complicated by the down-select capabilities of this library...
        """
        self.cfg = cfg
        # Set default values into cfg if not provided by the user
        for key in self.OSW_DEFAULTS[self.cfg['stock_type']].keys():
            if self.cfg['baseline'][key] is None:
                self.cfg['baseline'][key] = self.OSW_DEFAULTS[self.cfg['stock_type']][key]
        if n_datapoints is not None:
            self.cfg['baseline']['n_datapoints'] = n_datapoints
        self.osw_attrs = {}

    def create_osw(self, sim_id, **kwargs):
        """
        Generate the OSW as defined by the user, of as per defaults defined in this class for the given stock type.\
        This function dispatches to other methods as defined by stock type and desired template for said stock type.\
        For the required kwargs for each OSW generation method please refer to the _create_ method for the generation\
        method of interest.
        :param sim_id: I think this is the simulation id that matches up against the buildstock.csv file.
        :param kwargs: Currently used keys include 'i', which may in fact be the simulation id, and 'upgrade_idx'.
        """
        method = '_create_' + self.cfg['stock_type'] + '_' + self.cfg['baseline']['osw_template'] + '_osw'
        method = getattr(self, method)(sim_id, **kwargs)
        return method

    def verify_kwargs(self, required_keys, defaults, kwargs):
        """
        Verify that the required kwargs are provided for a given method.
        :param required_keys: List of keys that are required by the method.
        :param defaults: Dictionary of default values with are generally acceptable unless otherwise defined.
        :param kwargs: Input kwargs passed to the method.
        """
        for key in required_keys:
            if key in kwargs.keys():
                self.osw_attrs[key] = kwargs[key]
            elif key in defaults.keys():
                self.osw_attrs[key] = defaults[key]
                warn('Required key `{}` was not provided - defaulting to {}'.format(key, defaults[key]))
            else:
                raise KeyError('Unable to find required attribute `{}`.'.format(key))

    def _create_residential_default_osw(self, sim_id, **kwargs):
        logger.debug('Validating inputs for create_residential_default_osw')
        required_keys = ['i', 'upgrade_idx']
        self.verify_kwargs(required_keys, {}, kwargs)
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))
        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': {
                        'building_id': self.osw_attrs['i'],
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

        if self.osw_attrs['upgrade_idx'] is not None:
            measure_d = self.cfg['upgrades'][self.osw_attrs['upgrade_idx']]
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
                        self._make_apply_logic_arg(option['apply_logic'])
                for cost_num, cost in enumerate(option['costs'], 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                apply_upgrade_measure['package_apply_logic'] = self._make_apply_logic_arg(
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

    def _make_apply_logic_arg(self, logic):
        if isinstance(logic, dict):
            assert (len(logic) == 1)
            key = list(logic.keys())[0]
            if key == 'and':
                return '(' + '&&'.join(map(self._make_apply_logic_arg, logic[key])) + ')'
            elif key == 'or':
                return '(' + '||'.join(map(self._make_apply_logic_arg, logic[key])) + ')'
            elif key == 'not':
                return '!' + self._make_apply_logic_arg(logic[key])
        elif isinstance(logic, list):
            return '(' + '&&'.join(map(self._make_apply_logic_arg, logic)) + ')'
        elif isinstance(logic, str):
            return logic

    def _create_commercial_default_osw(self, sim_id, **kwargs):
        logger.debug('Validating inputs for _create_commercial_default_osw')
        required_keys = []
        self.verify_kwargs(required_keys, {}, kwargs)
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))
        raise NotImplementedError
