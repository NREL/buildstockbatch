import os
import tempfile
import logging
import shutil
import zipfile
import datetime as dt
from copy import deepcopy

import requests
import yaml


class BuildStockBatchBase(object):

    OS_VERSION = '2.6.0'
    OS_SHA = '8c81faf8bc'

    def __init__(self, project_filename):
        self.project_filename = os.path.abspath(project_filename)
        with open(self.project_filename, 'r') as f:
            self.cfg = yaml.load(f)
        self._weather_dir = None

        # Get the weather files
        local_weather_dir = os.path.join(self.project_dir, 'weather')
        for filename in os.listdir(local_weather_dir):
            shutil.copy(os.path.join(local_weather_dir, filename), self.weather_dir)
        if 'weather_files_path' in self.cfg:
            logging.debug('Copying weather files')
            if os.path.isabs(self.cfg['weather_files_path']):
                weather_file_path = os.path.abspath(self.cfg['weather_files_path'])
            else:
                weather_file_path = os.path.abspath(
                    os.path.join(
                        os.path.dirname(self.project_filename),
                        self.cfg['weather_files_path']
                    )
                )
            with zipfile.ZipFile(weather_file_path, 'r') as zf:
                logging.debug('Extracting weather files to: {}'.format(self.weather_dir))
                zf.extractall(self.weather_dir)
        else:
            logging.debug('Downloading weather files')
            r = requests.get(self.cfg['weather_files_url'], stream=True)
            with tempfile.TemporaryFile() as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.seek(0)
                with zipfile.ZipFile(f, 'r') as zf:
                    logging.debug('Extracting weather files to: {}'.format(self.weather_dir))
                    zf.extractall(self.weather_dir)

    @property
    def weather_dir(self):
        if self._weather_dir is None:
            self._weather_dir = tempfile.TemporaryDirectory(dir=self.project_dir, prefix='weather')
        return self._weather_dir.name

    @property
    def buildstock_dir(self):
        if os.path.isabs(self.cfg['buildstock_directory']):
            d = os.path.abspath(self.cfg['buildstock_directory'])
        else:
            d = os.path.abspath(
                os.path.join(
                    os.path.dirname(self.project_filename),
                    self.cfg['buildstock_directory']
                )
            )
        # logging.debug('buildstock_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    @property
    def project_dir(self):
        d = os.path.abspath(
            os.path.join(self.buildstock_dir, self.cfg['project_directory'])
        )
        # logging.debug('project_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    def run_sampling(self):
        raise NotImplementedError

    def run_batch(self):
        raise NotImplementedError

    @staticmethod
    def create_osw(sim_id, cfg, i, upgrade_idx):
        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': {
                        'building_id': i,
                        'workflow_json': 'measure-info.json',
                        'sample_weight': cfg['baseline']['n_buildings_represented'] / cfg['baseline']['n_datapoints']
                    }
                },
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
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
            'seed_file': 'seeds/EmptySeedModel.osm',
            'weather_file': 'weather/Placeholder.epw'
        }

        if upgrade_idx is not None:
            measure_d = cfg['upgrades'][upgrade_idx]
            apply_upgrade_measure = {
                'measure_dir_name': 'ApplyUpgrade',
                'arguments': {
                    'upgrade_name': measure_d['upgrade_name'],
                    'run_measure': 1
                }
            }
            for opt_num, option in enumerate(measure_d['options'], 1):
                apply_upgrade_measure['arguments']['option_{}'.format(opt_num)] = option['option']
                for arg in ('apply_logic', 'lifetime'):
                    if arg not in option:
                        continue
                    apply_upgrade_measure['arguments']['option_{}_{}'.format(opt_num, arg)] = option[arg]
                for cost_num, cost in enumerate(option['costs'], 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                apply_upgrade_measure['package_apply_logic'] = measure_d['package_apply_logic']

            osw['steps'].insert(1, apply_upgrade_measure)

        if 'timeseries_csv_export' in cfg:
            timeseries_measure = {
                'measure_dir_name': 'TimeseriesCSVExport',
                'arguments': deepcopy(cfg['timeseries_csv_export'])
            }
            timeseries_measure['arguments']['output_variables'] = \
                ','.join(cfg['timeseries_csv_export']['output_variables'])
            osw['steps'].insert(-1, timeseries_measure)

        return osw