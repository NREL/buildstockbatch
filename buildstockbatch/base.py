import os
import tempfile
import logging
import shutil
import zipfile
import datetime as dt
from copy import deepcopy
import re
import json
import subprocess
import gzip

import requests
import yaml
import dask
import dask.bag as db
from dask.distributed import Client


def read_data_point_out_json(filename):
    try:
        with open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, NotADirectoryError):
        return None
    else:
        d['_id'] = os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(filename))))
        return d


def to_camelcase(x):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def flatten_json(d):
    new_d = {
        '_id': d['_id'],
    }
    cols_to_keep = {
        'ApplyUpgrade': [
            'upgrade_name',
            'applicable'
        ]
    }
    for k1, k2s in cols_to_keep.items():
        for k2 in k2s:
            new_d['{}.{}'.format(k1, k2)] = d.get(k1, {}).get(k2)
    for k1 in ('BuildExistingModel', 'SimulationOutputReport'):
        for k2, v in d.get(k1, {}).items():
            new_d['{}.{}'.format(k1, k2)] = v
    return new_d


class BuildStockBatchBase(object):

    OS_VERSION = '2.6.0'
    OS_SHA = '8c81faf8bc'

    def __init__(self, project_filename):
        self.project_filename = os.path.abspath(project_filename)
        with open(self.project_filename, 'r') as f:
            self.cfg = yaml.load(f)
        self._weather_dir = None

        # Call property to create directory and copy weather files there
        _ = self.weather_dir

    def _get_weather_files(self):
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
            self._get_weather_files()
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

    @property
    def results_dir(self):
        raise NotImplementedError

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

    @staticmethod
    def cleanup_sim_dir(sim_dir):

        # Gzip the timeseries data
        timeseries_filename = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
        if os.path.isfile(timeseries_filename):
            with open(timeseries_filename, 'rb') as f_in:
                with gzip.open(timeseries_filename + '.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(timeseries_filename)

        # Remove files already in data_point.zip
        zipfilename = os.path.join(sim_dir, 'run', 'data_point.zip')
        enduse_timeseries_in_zip = False
        timeseries_filename_base = os.path.basename(timeseries_filename)
        if os.path.isfile(zipfilename):
            with zipfile.ZipFile(zipfilename, 'r') as zf:
                for filename in zf.namelist():
                    for filepath in (os.path.join(sim_dir, 'run', filename), os.path.join(sim_dir, filename)):
                        if os.path.exists(filepath):
                            os.remove(filepath)
                    if filename == timeseries_filename_base:
                        enduse_timeseries_in_zip = True

            # Remove csv file from data_point.zip
            # TODO: make this windows compatible
            if enduse_timeseries_in_zip:
                subprocess.run(
                    ['zip', '-d', zipfilename, timeseries_filename_base],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )

        # Remove reports dir
        reports_dir = os.path.join(sim_dir, 'reports')
        if os.path.isdir(reports_dir):
            shutil.rmtree(reports_dir)

    @staticmethod
    def _read_data_point_out_json(filename):
        with open(filename, 'r') as f:
            d = json.load(f)
        d['_id'] = os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(filename))))
        return d

    @staticmethod
    def to_camelcase(x):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def get_dask_client(self):
        return Client()

    def process_results(self):
        client = self.get_dask_client()
        results_dir = self.results_dir

        logging.debug('Creating Dask Dataframe of results')
        datapoint_output_jsons = db.from_sequence(os.listdir(results_dir), partition_size=500). \
            map(lambda x: os.path.join(results_dir, x, 'run', 'data_point_out.json'))
        df_d = datapoint_output_jsons.map(read_data_point_out_json).filter(lambda x: x is not None).\
            map(flatten_json).to_dataframe().rename(columns=to_camelcase)

        logging.debug('Computing Dask Dataframe')
        df = df_d.compute()

        logging.debug('Saving as csv')
        df.to_csv(os.path.join(results_dir, 'results.csv'))
        logging.debug('Saving as feather')
        df.reset_index().to_feather(os.path.join(results_dir, 'results.feather'))
