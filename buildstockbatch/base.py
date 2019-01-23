# -*- coding: utf-8 -*-

"""
buildstockbatch.base
~~~~~~~~~~~~~~~
This is the base class mixed into the deployment specific classes (i.e. peregrine, localdocker)

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from collections import defaultdict
import dask.bag as db
from dask.distributed import Client
import dask
import datetime as dt
from glob import glob
import gzip
import json
from json.decoder import JSONDecodeError
import logging
import math
import numpy as np
import os
import pandas as pd
import re
import requests
import shutil
import tempfile
import yaml
import zipfile

from .workflow_generator import ResidentialDefaultWorkflowGenerator, CommercialDefaultWorkflowGenerator

logger = logging.getLogger(__name__)


def read_data_point_out_json(filename):
    try:
        with open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, NotADirectoryError, json.JSONDecodeError):
        return None
    else:
        d['_id'] = os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(filename))))
        return d


def to_camelcase(x):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def flatten_datapoint_json(d):
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


def read_out_osw(filename):
    try:
        with open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, NotADirectoryError, JSONDecodeError):
        return None
    else:
        out_d = {}
        out_d['_id'] = os.path.basename(os.path.dirname(os.path.abspath(filename)))
        keys_to_copy = [
            'started_at',
            'completed_at',
            'completed_status'
        ]
        try:
            for key in keys_to_copy:
                out_d[key] = d[key]
            return out_d
        except KeyError:
            return None


class BuildStockBatchBase(object):

    OS_VERSION = '2.7.0'
    OS_SHA = '544f363db5'
    LOGO = '''
     _ __         _     __,              _ __
    ( /  )    o  //   /(    _/_       / ( /  )     _/_    /
     /--< , ,,  // __/  `.  /  __ _, /<  /--< __,  /  _, /
    /___/(_/_(_(/_(_/_(___)(__(_)(__/ |_/___/(_/(_(__(__/ /_
      Executing BuildStock projects with grace since 2018

'''

    def __init__(self, project_filename):
        self.project_filename = os.path.abspath(project_filename)
        with open(self.project_filename, 'r') as f:
            self.cfg = yaml.load(f)
        if 'stock_type' not in self.cfg.keys():
            raise KeyError('Key `stock_type` not specified in project file `{}`'.format(project_filename))
        elif (self.stock_type != 'residential') & (self.stock_type != 'commercial'):
            raise KeyError('Key `{}` for value `stock_type` not recognized in `{}`'.format(self.cfg['stock_type'],
                                                                                           project_filename))
        self._weather_dir = None
        # Call property to create directory and copy weather files there
        _ = self.weather_dir  # noqa: F841
        # Load in overriding OS_VERSION and OS_SHA arguments if they exist in the YAML
        if 'os_version' in self.cfg.keys():
            self.OS_VERSION = self.cfg['os_version']
        if 'os_sha' in self.cfg.keys():
            self.OS_VERSION = self.cfg['os_sha']

        self.sampler = None

    def _get_weather_files(self):
        local_weather_dir = os.path.join(self.project_dir, 'weather')
        for filename in os.listdir(local_weather_dir):
            shutil.copyfile(os.path.join(local_weather_dir, filename), self.weather_dir)
        if 'weather_files_path' in self.cfg:
            logger.debug('Copying weather files')
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
                logger.debug('Extracting weather files to: {}'.format(self.weather_dir))
                zf.extractall(self.weather_dir)
        else:
            logger.debug('Downloading weather files')
            r = requests.get(self.cfg['weather_files_url'], stream=True)
            with tempfile.TemporaryFile() as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.seek(0)
                with zipfile.ZipFile(f, 'r') as zf:
                    logger.debug('Extracting weather files to: {}'.format(self.weather_dir))
                    zf.extractall(self.weather_dir)

    @property
    def stock_type(self):
        return self.cfg['stock_type']

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
        # logger.debug('buildstock_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    @property
    def project_dir(self):
        d = os.path.abspath(
            os.path.join(self.buildstock_dir, self.cfg['project_directory'])
        )
        # logger.debug('project_dir = {}'.format(d))
        assert(os.path.isdir(d))
        return d

    @property
    def results_dir(self):
        raise NotImplementedError

    def run_sampling(self, n_datapoints=None):
        if n_datapoints is None:
            n_datapoints = self.cfg['baseline']['n_datapoints']
        return self.sampler.run_sampling(n_datapoints)

    def run_batch(self):
        raise NotImplementedError

    @classmethod
    def downselect_logic(cls, df, logic):
        if isinstance(logic, dict):
            assert (len(logic) == 1)
            key = list(logic.keys())[0]
            values = logic[key]
            if key == 'and':
                retval = cls.downselect_logic(df, values[0])
                for value in values[1:]:
                    retval &= cls.downselect_logic(df, value)
                return retval
            elif key == 'or':
                retval = cls.downselect_logic(df, values[0])
                for value in values[1:]:
                    retval |= cls.downselect_logic(df, value)
                return retval
            elif key == 'not':
                return ~cls.downselect_logic(df, values)
        elif isinstance(logic, list):
            retval = cls.downselect_logic(df, logic[0])
            for value in logic[1:]:
                retval &= cls.downselect_logic(df, value)
            return retval
        elif isinstance(logic, str):
            key, value = logic.split('|')
            return df[key] == value

    def downselect(self):
        downselect_resample = self.cfg['downselect'].get('resample', True)
        if downselect_resample:
            logger.debug('Performing initial sampling to figure out number of samples for downselect')
            n_samples_init = 350000
            buildstock_csv_filename = self.run_sampling(n_samples_init)
            df = pd.read_csv(buildstock_csv_filename, index_col=0)
            df_new = df[self.downselect_logic(df, self.cfg['downselect'])]
            downselected_n_samples_init = df_new.shape[0]
            n_samples = math.ceil(self.cfg['baseline']['n_datapoints'] * n_samples_init / downselected_n_samples_init)
            os.remove(buildstock_csv_filename)
        else:
            n_samples = self.cfg['baseline']['n_datapoints']
        buildstock_csv_filename = self.run_sampling(n_samples)
        with gzip.open(os.path.splitext(buildstock_csv_filename)[0] + '_orig.csv.gz', 'wb') as f_out:
            with open(buildstock_csv_filename, 'rb') as f_in:
                shutil.copyfileobj(f_in, f_out)
        df = pd.read_csv(buildstock_csv_filename, index_col=0)
        df_new = df[self.downselect_logic(df, self.cfg['downselect']['logic'])]
        if len(df_new.index) == 0:
            raise RuntimeError('There are no buildings left after the down select!')
        if downselect_resample:
            old_index_name = df_new.index.name
            df_new.index = np.arange(len(df_new)) + 1
            df_new.index.name = old_index_name
        df_new.to_csv(buildstock_csv_filename)
        return buildstock_csv_filename

    @staticmethod
    def create_osw(cfg, *args, **kwargs):
        if cfg['stock_type'] == 'residential':
            osw_generator = ResidentialDefaultWorkflowGenerator(cfg)
        else:
            assert(cfg['stock_type'] == 'commercial')
            osw_generator = CommercialDefaultWorkflowGenerator(cfg)
        return osw_generator.create_osw(*args, **kwargs)

    @staticmethod
    def cleanup_sim_dir(sim_dir, allocation):

        # Convert the timeseries data to parquet
        timeseries_filename = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
        if os.path.isfile(timeseries_filename):
            tsdf = pd.read_csv(timeseries_filename, parse_dates=['Time'])
            tsdf.to_parquet(os.path.splitext(timeseries_filename)[0] + '.parquet', engine='pyarrow', flavor='spark')

        # Remove files already in data_point.zip
        zipfilename = os.path.join(sim_dir, 'run', 'data_point.zip')
        if os.path.isfile(zipfilename):
            with zipfile.ZipFile(zipfilename, 'r') as zf:
                for filename in zf.namelist():
                    for filepath in (os.path.join(sim_dir, 'run', filename), os.path.join(sim_dir, filename)):
                        if os.path.exists(filepath):
                            os.remove(filepath)

        # Remove reports dir
        reports_dir = os.path.join(sim_dir, 'reports')
        if os.path.isdir(reports_dir):
            shutil.rmtree(reports_dir)

        # Set permissions and allocation ownership
        to_modify = [y for x in os.walk(sim_dir) for y in glob(os.path.join(x[0], '*.*'))]
        try:
            _ = [os.chmod(file, 0o664) for file in to_modify]
            _ = [shutil.chown(file, group=allocation) for file in to_modify]
        except:
            logger.warn('Unable to chmod/chown results of simulation in directory {}'.format(sim_dir))

    def get_dask_client(self):
        return Client()

    def process_results(self):
        client = self.get_dask_client()  # noqa: F841
        results_dir = self.results_dir

        results_by_upgrade = defaultdict(list)
        for item in os.listdir(results_dir):
            m = re.match(r'bldg(\d+)up(\d+)', item)
            if not m:
                continue
            building_id, upgrade_id = map(int, m.groups())
            results_by_upgrade[upgrade_id].append(item)

        store = pd.HDFStore(os.path.join(results_dir, 'results.h5'), complevel=9, complib='blosc:snappy')

        for upgrade_id, sim_dir_list in results_by_upgrade.items():

            logger.info('Computing results for upgrade {} with {} simulations'.format(upgrade_id, len(sim_dir_list)))

            datapoint_output_jsons = db.from_sequence(sim_dir_list, partition_size=500).\
                map(lambda x: os.path.join(results_dir, x, 'run', 'data_point_out.json')).\
                map(read_data_point_out_json).\
                filter(lambda x: x is not None)

            meta = pd.DataFrame(list(
                datapoint_output_jsons.filter(lambda x: 'SimulationOutputReport' in x.keys()).
                map(flatten_datapoint_json).take(10)
            ))
            if meta.shape == (0, 0):
                meta = None

            data_point_out_df_d = datapoint_output_jsons.map(flatten_datapoint_json).\
                to_dataframe(meta=meta).rename(columns=to_camelcase)

            out_osws = db.from_sequence(sim_dir_list, partition_size=500).\
                map(lambda x: os.path.join(results_dir, x, 'out.osw'))

            out_osw_df_d = out_osws.map(read_out_osw).filter(lambda x: x is not None).to_dataframe()

            data_point_out_df, out_osw_df = dask.compute(data_point_out_df_d, out_osw_df_d)

            results_df = out_osw_df.merge(data_point_out_df, how='left', on='_id')
            results_df['build_existing_model.building_id'] = results_df['build_existing_model.building_id'].\
                astype('int')
            cols_to_remove = (
                'build_existing_model.weight',
                'simulation_output_report.weight',
                'build_existing_model.workflow_json',
                'simulation_output_report.upgrade_name'
            )
            for col in cols_to_remove:
                if col in results_df.columns:
                    del results_df[col]
            for col in ('started_at', 'completed_at'):
                results_df[col] = results_df[col].map(lambda x: dt.datetime.strptime(x, '%Y%m%dT%H%M%SZ'))

            if upgrade_id > 0:
                cols_to_keep = list(
                    filter(lambda x: not re.match(r'build_existing_model\.(?!building_id)', x), results_df.columns)
                )
                results_df = results_df[cols_to_keep]

            logger.debug('Saving to csv.gz')
            csv_filename = os.path.join(results_dir, 'results_up{:02d}.csv.gz'.format(upgrade_id))
            with gzip.open(csv_filename, 'wt', encoding='utf-8') as f:
                results_df.to_csv(f, index=False)

            logger.debug('Saving to parquet')
            results_df.to_parquet(
                os.path.join(results_dir, 'results_up{:02d}.parquet'.format(upgrade_id)),
                engine='pyarrow',
                flavor='spark'
            )

            logger.debug('Categorizing variables')
            logger.debug('memory before categoricals: {:.1f}MB'.format(results_df.memory_usage(deep=True).sum() / 1e6))
            for col in results_df.columns:
                if results_df[col].dtype == 'object' and (col.startswith('build_existing_model.') or
                                                          col.startswith('apply_upgrade.') or
                                                          col == 'completed_status'):
                    results_df[col] = results_df[col].astype('category')
            logger.debug('memory after categoricals: {:.1f}MB'.format(results_df.memory_usage(deep=True).sum() / 1e6))

            logger.debug('Saving to HDF5')
            store.put('upgrade/{}'.format(upgrade_id), results_df, format='table', append=False)

        store.close()
