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
import gzip
import json
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
import time
import sys
import random

from .workflow_generator import ResidentialDefaultWorkflowGenerator, CommercialDefaultWorkflowGenerator

logger = logging.getLogger(__name__)


class SimulationExists(Exception):
    pass


def read_data_point_out_json(filename):
    try:
        with open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, NotADirectoryError, json.JSONDecodeError):
        return None
    else:
        if 'SimulationOutputReport' not in d:
            d['SimulationOutputReport'] = {'applicable': False}
        return d


def to_camelcase(x):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def flatten_datapoint_json(d):
    new_d = {}
    cols_to_keep = {
        'ApplyUpgrade': [
            'upgrade_name',
            'applicable'
        ]
    }
    for k1, k2s in cols_to_keep.items():
        for k2 in k2s:
            new_d[f'{k1}.{k2}'] = d.get(k1, {}).get(k2)

    # copy over all the key and values from BuildExistingModel
    col1 = 'BuildExistingModel'
    for k, v in d.get(col1, {}).items():
        new_d[f'{col1}.{k}'] = v

    # if there are some key, values in BuildingCharacteristicsReport that aren't part of BuildExistingModel, copy them
    # and make it part of BuildExistingModel
    col2 = 'BuildingCharacteristicsReport'
    for k, v in d.get(col2, {}).items():
        if k not in d.get(col1, {}):
            new_d[f'{col1}.{k}'] = v  # Using col1 to make it part of BuildExistingModel

    new_d['building_id'] = new_d['BuildExistingModel.building_id']
    del new_d['BuildExistingModel.building_id']

    return new_d


def read_out_osw(filename):
    try:
        with open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, NotADirectoryError, json.JSONDecodeError):
        return None
    else:
        out_d = {}
        keys_to_copy = [
            'started_at',
            'completed_at',
            'completed_status'
        ]
        for key in keys_to_copy:
            out_d[key] = d[key]
        for step in d['steps']:
            if step['measure_dir_name'] == 'BuildExistingModel':
                out_d['building_id'] = step['arguments']['building_id']
        return out_d


class BuildStockBatchBase(object):

    OS_VERSION = '2.8.0'
    OS_SHA = 'a7a1f79e98'
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
            self.cfg = yaml.load(f, Loader=yaml.SafeLoader)
        if 'stock_type' not in self.cfg.keys():
            raise KeyError('Key `stock_type` not specified in project file `{}`'.format(project_filename))
        elif (self.stock_type != 'residential') & (self.stock_type != 'commercial'):
            raise KeyError('Key `{}` for value `stock_type` not recognized in `{}`'.format(self.cfg['stock_type'],
                                                                                           project_filename))
        self._weather_dir = None
        # Call property to create directory and copy weather files there
        _ = self.weather_dir  # noqa: F841

        self.sampler = None

    def _get_weather_files(self):
        local_weather_dir = os.path.join(self.project_dir, 'weather')
        for filename in os.listdir(local_weather_dir):
            shutil.copy(os.path.join(local_weather_dir, filename), self.weather_dir)
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
            self._weather_dir = tempfile.TemporaryDirectory(dir=self.results_dir, prefix='weather')
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
            df_new = df[self.downselect_logic(df, self.cfg['downselect']['logic'])]
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
    def make_sim_dir(building_id, upgrade_idx, base_dir, overwrite_existing=False):
        real_upgrade_idx = 0 if upgrade_idx is None else upgrade_idx + 1
        sim_id = 'bldg{:07d}up{:02d}'.format(building_id, real_upgrade_idx)

        # Check to see if the simulation is done already and skip it if so.
        sim_dir = os.path.join(base_dir, 'up{:02d}'.format(real_upgrade_idx), 'bldg{:07d}'.format(building_id))
        if os.path.exists(sim_dir):
            if os.path.exists(os.path.join(sim_dir, 'run', 'finished.job')):
                raise SimulationExists('{} exists and finished successfully'.format(sim_id))
            elif os.path.exists(os.path.join(sim_dir, 'run', 'failed.job')):
                raise SimulationExists('{} exists and failed'.format(sim_id))
            else:
                shutil.rmtree(sim_dir)

        # Create the simulation directory
        os.makedirs(sim_dir)

        return sim_id, sim_dir

    @staticmethod
    def cleanup_sim_dir(sim_dir):

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

    def get_dask_client(self):
        return Client()

    def process_results(self):
        client = self.get_dask_client()  # noqa: F841

        sim_out_dir = os.path.join(self.results_dir, 'simulation_output')

        results_csvs_dir = os.path.join(self.results_dir, 'results_csvs')
        parquet_dir = os.path.join(self.results_dir, 'parquet')
        ts_dir = os.path.join(self.results_dir, 'parquet', 'timeseries')

        # clear and create the postprocessing results directories
        for dr in [results_csvs_dir, parquet_dir, ts_dir]:
            if os.path.exists(dr):
                shutil.rmtree(dr)
            os.makedirs(dr)

        results_by_upgrade = defaultdict(list)
        all_dirs = list()
        for item in os.listdir(sim_out_dir):
            m = re.match(r'up(\d+)', item)
            if not m:
                continue
            upgrade_id = int(m.group(1))
            for subitem in os.listdir(os.path.join(sim_out_dir, item)):
                m = re.match(r'bldg(\d+)', subitem)
                if not m:
                    continue
                full_path = os.path.join(item, subitem)
                results_by_upgrade[upgrade_id].append(full_path)
                all_dirs.append(full_path)

        base_results_df = None
        # create the results.csv and results.parquet files
        for upgrade_id, sim_dir_list in results_by_upgrade.items():

            logger.info('Computing results for upgrade {} with {} simulations'.format(upgrade_id, len(sim_dir_list)))

            datapoint_output_jsons = db.from_sequence(sim_dir_list, partition_size=500).\
                map(lambda x: os.path.join(sim_out_dir, x, 'run', 'data_point_out.json')).\
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
                map(lambda x: os.path.join(sim_out_dir, x, 'out.osw'))

            out_osw_df_d = out_osws.map(read_out_osw).filter(lambda x: x is not None).to_dataframe()

            data_point_out_df, out_osw_df = dask.compute(data_point_out_df_d, out_osw_df_d)

            results_df = out_osw_df.merge(data_point_out_df, how='left', on='building_id')
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
                    filter(lambda x: not x.startswith('build_existing_model.'), results_df.columns)
                )
                results_df = results_df[cols_to_keep]

            # Save to CSV
            logger.debug('Saving to csv.gz')
            csv_filename = os.path.join(results_csvs_dir, 'results_up{:02d}.csv.gz'.format(upgrade_id))
            with gzip.open(csv_filename, 'wt', encoding='utf-8') as f:
                results_df.to_csv(f, index=False)

            # Save to parquet
            logger.debug('Saving to parquet')
            if upgrade_id == 0:
                results_parquet_dir = os.path.join(parquet_dir, 'baseline')
                base_results_df = results_df.set_index('building_id')
            else:
                results_parquet_dir = os.path.join(parquet_dir, 'upgrades', 'upgrade={}'.format(upgrade_id))
            os.makedirs(results_parquet_dir, exist_ok=True)
            results_df.to_parquet(
                os.path.join(results_parquet_dir, 'results_up{:02d}.parquet'.format(upgrade_id)),
                engine='pyarrow',
                flavor='spark'
            )

        # find the avg size of time_series parqeut files
        total_size = 0
        count = 0
        sample_size = 10 if len(all_dirs) >= 10 else len(all_dirs)
        for rnd_ts_index in random.sample(range(len(all_dirs)), sample_size):
            full_path = os.path.join(sim_out_dir, all_dirs[rnd_ts_index], 'run', 'enduse_timeseries.parquet')
            try:
                pq = pd.read_parquet(full_path)
                total_size += sys.getsizeof(pq)
                count += 1
            except OSError:
                logger.warning(f" Time series file does not exist: {full_path}")
                continue

        avg_parquet_size = total_size / count

        group_size = int(1.3*1024*1024*1024 / avg_parquet_size)
        if group_size < 1:
            group_size = 1

        logger.info(f"Each parquet file is {avg_parquet_size / (1024 * 1024) :.2f} in memory. \n" +
                    f"Combining {group_size} of them together, so that the size in memory is around 1.5 GB")

        def bldg_group(directory_name):
            mtch = re.search('up([0-9]+)/bldg([0-9]+)', directory_name)
            assert mtch, f"list of directories passed should be properly formatted as: 'up([0-9]+)/bldg([0-9]+)'. " \
                         f"Got {directory_name}"
            group = 'up' + mtch[1] + '_Group' + str(int(mtch[2]) // group_size)
            return group

        def directory_name_append(name1, name2):
            if name1 is None:
                return name2
            else:
                return name1 + '\n' + name2

        def write_output(group_pq):
            group = group_pq[0]
            folders = group_pq[1]

            try:
                upgrade, groupname = group.split('_')
                m = re.match(r'up(\d+)', upgrade)
                upgrade_id = int(m.group(1))
            except (ValueError, AttributeError):
                logger.error(f"The group labels created from bldg_group function should "
                             f"have 'up([0-9]+)_GroupXX' format. Found: {group}")
                return

            folder_path = os.path.join(ts_dir, f"upgrade={upgrade_id}")
            os.makedirs(folder_path, exist_ok=True)

            file_path = os.path.join(folder_path, str(groupname) + '.parquet')
            parquets = []
            for folder in folders.split():
                full_path = os.path.join(sim_out_dir, folder, 'run', 'enduse_timeseries.parquet')
                if not os.path.isfile(full_path):
                    continue
                new_pq = pd.read_parquet(full_path, engine='pyarrow')
                new_pq.rename(columns=to_camelcase, inplace=True)

                # Scale the timeseries by the number of units represented to make it per unit measurement
                building_id_match = re.search(r'bldg(\d+)', folder)
                assert building_id_match, f"The building results folder format should be: ~bldg(\\d+). Got: {folder} "
                building_id = int(building_id_match.group(1))
                assert building_id in base_results_df.index, f"Building id {building_id} not present in results.csv"
                new_pq['building_id'] = building_id
                units_represented = int(base_results_df.at[building_id, 'build_existing_model.units_represented'])
                new_pq['units_represented'] = units_represented

                parquets.append(new_pq)

            pq_size = (sum([sys.getsizeof(pq) for pq in parquets]) + sys.getsizeof(parquets)) / (1024 * 1024)
            logger.debug(f"{group}: list of {len(parquets)} parquets is consuming "
                         f"{pq_size:.2f} MB memory on a dask worker process.")
            pq = pd.concat(parquets)
            logger.debug(f"The concatenated parquet file is consuming {sys.getsizeof(pq) / (1024 * 1024) :.2f} MB.")
            pq.to_parquet(file_path, engine='pyarrow', flavor='spark')

        directory_bags = db.from_sequence(all_dirs).foldby(bldg_group, directory_name_append, initial=None,
                                                           combine=directory_name_append)
        bags = directory_bags.compute()

        logger.info("Combining the parquets")
        t = time.time()
        write_file = db.from_sequence(bags).map(write_output)
        write_file.compute()
        diff = time.time() - t
        logger.info(f"Took {diff:.2f} seconds")
