# -*- coding: utf-8 -*-

"""
buildstockbatch.base
~~~~~~~~~~~~~~~
This is the base class mixed into the deployment specific classes (i.e. eagle, localdocker)

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from dask.distributed import Client
import gzip
import logging
import math
import numpy as np
import os
import pandas as pd
import requests
import shutil
import tempfile
import yaml
import yamale
import zipfile

from buildstockbatch.__version__ import __schema_version__
from .workflow_generator import ResidentialDefaultWorkflowGenerator, CommercialDefaultWorkflowGenerator
from .postprocessing import combine_results, upload_results, create_athena_tables, write_dataframe_as_parquet

logger = logging.getLogger(__name__)


class SimulationExists(Exception):
    pass


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

        if 'buildstock_csv' in self.cfg['baseline']:
            buildstock_csv = self.path_rel_to_projectfile(self.cfg['baseline']['buildstock_csv'])
            if not os.path.exists(buildstock_csv):
                raise FileNotFoundError('The buildstock.csv file does not exist at {}'.format(buildstock_csv))
            df = pd.read_csv(buildstock_csv)
            n_datapoints = self.cfg['baseline'].get('n_datapoints', df.shape[0])
            self.cfg['baseline']['n_datapoints'] = n_datapoints
            if n_datapoints != df.shape[0]:
                raise RuntimeError(
                    'A buildstock_csv was provided, so n_datapoints for sampling should not be provided or should be '
                    'equal to the number of rows in the buildstock.csv file. Remove or comment out '
                    'baseline->n_datapoints from your project file.'
                )
            if 'downselect' in self.cfg:
                raise RuntimeError(
                    'A buildstock_csv was provided, which isn\'t compatible with downselecting.'
                    'Remove or comment out the downselect key from your project file.'
                )

        self.sampler = None

    def path_rel_to_projectfile(self, x):
        if os.path.isabs(x):
            return os.path.abspath(x)
        else:
            return os.path.abspath(os.path.join(os.path.dirname(self.project_filename), x))

    def _get_weather_files(self):
        local_weather_dir = os.path.join(self.project_dir, 'weather')
        for filename in os.listdir(local_weather_dir):
            shutil.copy(os.path.join(local_weather_dir, filename), self.weather_dir)
        if 'weather_files_path' in self.cfg:
            logger.debug('Copying weather files')
            weather_file_path = self.path_rel_to_projectfile(self.cfg['weather_files_path'])
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
        d = self.path_rel_to_projectfile(self.cfg['buildstock_directory'])
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

    @property
    def output_dir(self):
        raise NotImplementedError

    def run_sampling(self, n_datapoints=None):
        if n_datapoints is None:
            n_datapoints = self.cfg['baseline']['n_datapoints']
        if 'buildstock_csv' in self.cfg['baseline']:
            buildstock_csv = self.path_rel_to_projectfile(self.cfg['baseline']['buildstock_csv'])
            destination_filename = self.sampler.csv_path
            if destination_filename != buildstock_csv:
                if os.path.exists(destination_filename):
                    os.remove(destination_filename)
                shutil.copy(
                    buildstock_csv,
                    destination_filename
                )
            return destination_filename
        else:
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
        timeseries_filepath = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
        if os.path.isfile(timeseries_filepath):
            tsdf = pd.read_csv(timeseries_filepath, parse_dates=['Time'])
            timeseries_filedir, timeseries_filename = os.path.split(timeseries_filepath)
            write_dataframe_as_parquet(tsdf, timeseries_filedir, os.path.splitext(timeseries_filename)[0] + '.parquet')

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

    @staticmethod
    def validate_project(project_file):
        assert(BuildStockBatchBase.validate_project_schema(project_file))
        assert(BuildStockBatchBase.validate_xor_schema_keys(project_file))
        logger.info('Base Validation Successful')
        return True

    @staticmethod
    def validate_project_schema(project_file):
        try:
            with open(project_file) as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
        except FileNotFoundError as err:
            print(f'Failed to load input yaml for validation')
            raise err
        schema_version = cfg.get('version', __schema_version__)
        version_schema = os.path.join(os.path.dirname(__file__), 'schemas', f'v{schema_version}.yaml')
        if not os.path.isfile(version_schema):
            print(f'Could not find validation schema for YAML version {cfg["version"]}')
            raise FileNotFoundError(version_schema)
        schema = yamale.make_schema(version_schema)
        data = yamale.make_data(project_file)
        return yamale.validate(schema, data)

    @staticmethod
    def validate_xor_schema_keys(project_file):
        try:
            with open(project_file) as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
        except FileNotFoundError as err:
            print(f'Failed to load input yaml for validation')
            raise err
        major, minor = cfg.get('version', __schema_version__).split('.')
        if int(major) >= 0:
            if int(minor) >= 0:
                if ('weather_files_url' in cfg.keys()) is \
                   ('weather_files_path' in cfg.keys()):
                    raise ValueError('Both/neither weather_files_url and weather_files_path found in yaml root')
                if ('n_datapoints' in cfg['baseline'].keys()) is \
                   ('buildstock_csv' in cfg['baseline'].keys()):
                    raise ValueError('Both/neither n_datapoints and buildstock_csv found in yaml baseline key')
        return True

    def get_dask_client(self):
        return Client()

    def process_results(self, skip_combine=False, force_upload=False):
        self.get_dask_client()  # noqa: F841

        if self.cfg.get('timeseries_csv_export', {}):
            skip_timeseries = False
        else:
            skip_timeseries = True

        aggregate_ts = self.cfg.get('postprocessing', {}).get('aggregate_timeseries', False)

        if not skip_combine:
            combine_results(self.results_dir, skip_timeseries=skip_timeseries, aggregate_timeseries=aggregate_ts)

        aws_conf = self.cfg.get('postprocessing', {}).get('aws', {})
        if 's3' in aws_conf or force_upload:
            s3_bucket, s3_prefix = upload_results(aws_conf, self.output_dir, self.results_dir)
            if 'athena' in aws_conf:
                create_athena_tables(aws_conf, self.output_dir, s3_bucket, s3_prefix)
