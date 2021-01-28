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
import difflib
from fsspec.implementations.local import LocalFileSystem
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
import csv
from collections import defaultdict, Counter
import xml.etree.ElementTree as ET

from buildstockbatch.__version__ import __schema_version__
from .workflow_generator import ResidentialDefaultWorkflowGenerator, CommercialDefaultWorkflowGenerator
from buildstockbatch import postprocessing

logger = logging.getLogger(__name__)


class SimulationExists(Exception):

    def __init__(self, msg, sim_id, sim_dir):
        super().__init__(msg)
        self.sim_id = sim_id
        self.sim_dir = sim_dir


class ValidationError(Exception):
    pass


class BuildStockBatchBase(object):

    DEFAULT_OS_VERSION = '2.9.1'
    DEFAULT_OS_SHA = '3472e8b799'
    LOGO = '''
     _ __         _     __,              _ __
    ( /  )    o  //   /(    _/_       / ( /  )     _/_    /
     /--< , ,,  // __/  `.  /  __ _, /<  /--< __,  /  _, /
    /___/(_/_(_(/_(_/_(___)(__(_)(__/ |_/___/(_/(_(__(__/ /_
      Executing BuildStock projects with grace since 2018

'''

    def __init__(self, project_filename):
        self.project_filename = os.path.abspath(project_filename)

        # Load project file to self.cfg
        self.cfg = self.get_project_configuration(project_filename)

        self.buildstock_dir = self.cfg['buildstock_directory']
        if not os.path.isdir(self.buildstock_dir):
            raise FileNotFoundError(f'buildstock_directory = {self.buildstock_dir} is not a directory.')
        self.project_dir = os.path.join(self.buildstock_dir, self.cfg['project_directory'])
        if not os.path.isdir(self.project_dir):
            raise FileNotFoundError(f'project_directory = {self.project_dir} is not a directory.')

        # To be set in subclasses
        self.sampler = None

        # Load in OS_VERSION and OS_SHA arguments if they exist in the YAML,
        # otherwise use defaults specified here.
        self.os_version = self.cfg.get('os_version', self.DEFAULT_OS_VERSION)
        self.os_sha = self.cfg.get('os_sha', self.DEFAULT_OS_SHA)
        logger.debug(f"Using OpenStudio version: {self.os_version} with SHA: {self.os_sha}")

    @staticmethod
    def path_rel_to_file(startfile, x):
        if os.path.isabs(x):
            return os.path.abspath(x)
        else:
            return os.path.abspath(os.path.join(os.path.dirname(startfile), x))

    def path_rel_to_projectfile(self, x):
        return self.path_rel_to_file(self.project_filename, x)

    def _get_weather_files(self):
        if 'weather_files_path' in self.cfg:
            logger.debug('Copying weather files')
            weather_file_path = self.cfg['weather_files_path']
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
        raise NotImplementedError

    @property
    def results_dir(self):
        raise NotImplementedError

    @property
    def output_dir(self):
        raise NotImplementedError

    @property
    def skip_baseline_sims(self):
        baseline_skip = self.cfg['baseline'].get('skip_sims', False)
        return baseline_skip

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
        logger.debug("Starting downselect sampling")
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
        df = pd.read_csv(buildstock_csv_filename, index_col=0, dtype='str')
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
        if os.path.exists(sim_dir) and not overwrite_existing:
            if os.path.exists(os.path.join(sim_dir, 'run', 'finished.job')):
                raise SimulationExists('{} exists and finished successfully'.format(sim_id), sim_id, sim_dir)
            elif os.path.exists(os.path.join(sim_dir, 'run', 'failed.job')):
                raise SimulationExists('{} exists and failed'.format(sim_id), sim_id, sim_dir)
            else:
                shutil.rmtree(sim_dir)

        # Create the simulation directory
        os.makedirs(sim_dir, exist_ok=overwrite_existing)

        return sim_id, sim_dir

    @staticmethod
    def cleanup_sim_dir(sim_dir, dest_fs, simout_ts_dir, upgrade_id, building_id):
        """Clean up the output directory for a single simulation.

        :param sim_dir: simulation directory
        :type sim_dir: str
        :param dest_fs: filesystem destination of timeseries parquet file
        :type dest_fs: fsspec filesystem
        :param simout_ts_dir: simulation_output/timeseries directory to deposit timeseries parquet file
        :type simout_ts_dir: str
        :param upgrade_id: upgrade number for the simulation 0 for baseline, etc.
        :type upgrade_id: int
        :param building_id: building id from buildstock.csv
        :type building_id: int
        """

        # Convert the timeseries data to parquet
        # and copy it to the results directory
        timeseries_filepath = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
        schedules_filepath = os.path.join(sim_dir, 'generated_files', 'schedules.csv')
        if os.path.isfile(timeseries_filepath):
            # Find the time columns present in the enduse_timeseries file
            possible_time_cols = ['time', 'Time', 'TimeDST', 'TimeUTC']
            cols = pd.read_csv(timeseries_filepath, index_col=False, nrows=0).columns.tolist()
            actual_time_cols = [c for c in cols if c in possible_time_cols]
            if not actual_time_cols:
                logger.error(f'Did not find any time column ({possible_time_cols}) in enduse_timeseries.csv.')
                raise RuntimeError(f'Did not find any time column ({possible_time_cols}) in enduse_timeseries.csv.')
            tsdf = pd.read_csv(timeseries_filepath, parse_dates=actual_time_cols)
            if os.path.isfile(schedules_filepath):
                schedules = pd.read_csv(schedules_filepath)
                schedules.rename(columns=lambda x: f'schedules_{x}', inplace=True)
                schedules['TimeDST'] = tsdf['Time']
                tsdf = tsdf.merge(schedules, how='left', on='TimeDST')
            postprocessing.write_dataframe_as_parquet(
                tsdf,
                dest_fs,
                f'{simout_ts_dir}/up{upgrade_id:02d}/bldg{building_id:07d}.parquet'
            )

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
            shutil.rmtree(reports_dir, ignore_errors=True)

    @staticmethod
    def validate_project(project_file):
        assert(BuildStockBatchBase.validate_project_schema(project_file))
        assert(BuildStockBatchBase.validate_misc_constraints(project_file))
        assert(BuildStockBatchBase.validate_xor_nor_schema_keys(project_file))
        assert(BuildStockBatchBase.validate_precomputed_sample(project_file))
        assert(BuildStockBatchBase.validate_reference_scenario(project_file))
        assert(BuildStockBatchBase.validate_measures_and_arguments(project_file))
        assert(BuildStockBatchBase.validate_options_lookup(project_file))
        assert(BuildStockBatchBase.validate_measure_references(project_file))
        assert(BuildStockBatchBase.validate_options_lookup(project_file))
        logger.info('Base Validation Successful')
        return True

    @classmethod
    def get_project_configuration(cls, project_file):
        try:
            with open(project_file) as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
        except FileNotFoundError as err:
            logger.error('Failed to load input yaml for validation')
            raise err

        # Set absolute paths
        cfg['buildstock_directory'] = cls.path_rel_to_file(project_file, cfg['buildstock_directory'])
        if 'precomputed_sample' in cfg.get('baseline', {}):
            cfg['baseline']['precomputed_sample'] = \
                cls.path_rel_to_file(project_file, cfg['baseline']['precomputed_sample'])
        if 'weather_files_path' in cfg:
            cfg['weather_files_path'] = cls.path_rel_to_file(project_file, cfg['weather_files_path'])

        return cfg

    @staticmethod
    def get_buildstock_dir(project_file, cfg):
        buildstock_dir = cfg["buildstock_directory"]
        if os.path.isabs(buildstock_dir):
            return os.path.abspath(buildstock_dir)
        else:
            return os.path.abspath(os.path.join(os.path.dirname(project_file), buildstock_dir))

    @staticmethod
    def validate_weather_files(project_file, buildstock_csv_filename, weather_dir):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        buildstock_dir = BuildStockBatchBase.get_buildstock_dir(project_file, cfg)
        options_lookup_path = f'{buildstock_dir}/resources/options_lookup.tsv'
        arg_vals = BuildStockBatchBase.get_weather_args_from_measure_dir(
            options_lookup_path,
            'ResidentialLocation')  # epws from buildstock.csv
        weather_dir_files = os.listdir(weather_dir)  # epws from weather file path
        weather_dir_files = [f for f in weather_dir_files if ".epw" in f]
        if len(set.intersection(set(weather_dir_files), set(arg_vals))) == 0:  # not files overlap
            raise ValidationError("Weather file arguments are invalid")
        else:
            return True

    @staticmethod
    def get_weather_args_from_measure_dir(lookup_file, measure_dir):
        weather_args = []
        with open(os.path.join(lookup_file)) as f:
            csv_reader = csv.reader(f, delimiter='\t')
            for row in csv_reader:
                if not row[2]:
                    continue
                if str(row[2]).lower() == measure_dir.lower():
                    if len(row) <= 3:
                        continue
                    for col in range(3, len(row), 1):
                        if row[col] is None or "=" not in str(row[col]):
                            continue
                        data = row[col].split("=")
                        arg_value = data[1]
                        if ".epw" in arg_value:
                            weather_args.append(arg_value)
                            break

        return weather_args

    @staticmethod
    def validate_project_schema(project_file):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        schema_version = cfg.get('schema_version', __schema_version__)
        version_schema = os.path.join(os.path.dirname(__file__), 'schemas', f'v{schema_version}.yaml')
        if not os.path.isfile(version_schema):
            logger.error(f'Could not find validation schema for YAML version {schema_version}')
            raise FileNotFoundError(version_schema)
        schema = yamale.make_schema(version_schema)
        data = yamale.make_data(project_file, parser='ruamel')
        return yamale.validate(schema, data, strict=True)

    @staticmethod
    def validate_misc_constraints(project_file):
        # validate other miscellaneous constraints
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        if 'precomputed_sample' in cfg['baseline']:
            if cfg.get('downselect', {'resample': False}).get('resample', True):
                raise ValidationError("Downselect with resampling cannot be used when using precomputed buildstock_csv."
                                      "\nPlease set resample: False in downselect or use a different sampler.")

        if cfg.get('postprocessing', {}).get('aggregate_timeseries', False):
            logger.warning('aggregate_timeseries has been deprecated and will be removed in a future version.')

        return True

    @staticmethod
    def validate_xor_nor_schema_keys(project_file):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        major, minor = cfg.get('version', __schema_version__).split('.')
        if int(major) >= 0:
            if int(minor) >= 0:
                # xor
                if ('weather_files_url' in cfg.keys()) is \
                   ('weather_files_path' in cfg.keys()):
                    raise ValidationError('Both/neither weather_files_url and weather_files_path found in yaml root')

                # No precomputed sample key unless using precomputed sampling
                if cfg['baseline']['sampling_algorithm'] != 'precomputed' and 'precomputed_sample' in cfg['baseline']:
                    raise ValidationError(
                        'baseline.precomputed_sample is not allowed unless '
                        'baseline.sampling_algorithm = "precomputed".'
                    )
        return True

    @staticmethod
    def validate_precomputed_sample(project_file):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        if 'precomputed_sample' in cfg['baseline']:
            buildstock_csv = cfg['baseline']['precomputed_sample']
            if not os.path.exists(buildstock_csv):
                raise FileNotFoundError(buildstock_csv)
            buildstock_df = pd.read_csv(buildstock_csv)
            if buildstock_df.shape[0] != cfg['baseline']['n_datapoints']:
                raise RuntimeError(
                    f'`n_datapoints` does not match the number of rows in {buildstock_csv}. '
                    f'Please set `n_datapoints` to {buildstock_df.shape[0]}'
                )
        return True

    @staticmethod
    def get_measure_xml(xml_path):
        tree = ET.parse(xml_path)
        root = tree.getroot()
        return root

    @staticmethod
    def validate_measures_and_arguments(project_file):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        if cfg['stock_type'] != 'residential':  # FIXME: add comstock logic
            return True

        buildstock_dir = os.path.join(os.path.dirname(project_file), cfg["buildstock_directory"])
        measures_dir = f'{buildstock_dir}/measures'
        type_map = {'Integer': int, 'Boolean': bool, 'String': str, 'Double': float}

        measure_names = {
            'ResidentialSimulationControls': 'residential_simulation_controls',
            'BuildExistingModel': 'baseline',
            'SimulationOutputReport': 'simulation_output',
            'ServerDirectoryCleanup': None,
            'ApplyUpgrade': 'upgrades',
            'TimeseriesCSVExport': 'timeseries_csv_export'
        }
        if 'reporting_measures' in cfg.keys():
            for reporting_measure in cfg['reporting_measures']:
                measure_names[reporting_measure] = 'reporting_measures'

        error_msgs = ''
        warning_msgs = ''
        for measure_name in measure_names.keys():
            measure_path = os.path.join(measures_dir, measure_name)

            if measure_names[measure_name] in cfg.keys() or \
                    measure_names[measure_name] == 'residential_simulation_controls':
                # if they exist in the cfg, make sure they exist in the buildstock checkout
                if not os.path.exists(measure_path):
                    error_msgs += f"* {measure_name} does not exist in {buildstock_dir}. \n"

            # check the rest only if that measure exists in cfg
            if measure_names[measure_name] not in cfg.keys():
                continue

            # check argument value types for residential simulation controls and timeseries csv export measures
            if measure_name in ['ResidentialSimulationControls', 'SimulationOutputReport', 'TimeseriesCSVExport']:
                root = BuildStockBatchBase.get_measure_xml(os.path.join(measure_path, 'measure.xml'))
                expected_arguments = {}
                required_args_with_default = {}
                required_args_no_default = {}
                for argument in root.findall('./arguments/argument'):
                    name = argument.find('./name').text
                    expected_arguments[name] = []
                    required = argument.find('./required').text
                    default = argument.find('./default_value')
                    default = default.text if default is not None else None

                    if required == 'true' and not default:
                        required_args_no_default[name] = None
                    elif required == 'true':
                        required_args_with_default[name] = default

                    if argument.find('./type').text == 'Choice':
                        for choice in argument.findall('./choices/choice'):
                            for value in choice.findall('./value'):
                                expected_arguments[name].append(value.text)
                    else:
                        expected_arguments[name] = argument.find('./type').text

                for actual_argument_key in cfg[measure_names[measure_name]].keys():
                    if actual_argument_key not in expected_arguments.keys():
                        error_msgs += f"* Found unexpected argument key {actual_argument_key} for "\
                                      f"{measure_names[measure_name]} in yaml file. The available keys are: " \
                                      f"{list(expected_arguments.keys())}\n"
                        continue

                    required_args_no_default.pop(actual_argument_key, None)
                    required_args_with_default.pop(actual_argument_key, None)

                    actual_argument_value = cfg[measure_names[measure_name]][actual_argument_key]
                    expected_argument_type = expected_arguments[actual_argument_key]

                    if type(expected_argument_type) is not list:
                        try:
                            if type(actual_argument_value) is not list:
                                actual_argument_value = [actual_argument_value]

                            for val in actual_argument_value:
                                if not isinstance(val, type_map[expected_argument_type]):
                                    error_msgs += f"* Wrong argument value type for {actual_argument_key} for measure "\
                                                  f"{measure_names[measure_name]} in yaml file. Expected type:" \
                                                  f" {type_map[expected_argument_type]}, got: {val}" \
                                                  f" of type: {type(val)} \n"
                        except KeyError:
                            print(f"Found an unexpected argument value type: {expected_argument_type} for argument "
                                  f" {actual_argument_key} in measure {measure_name}.\n")
                    else:  # Choice
                        if actual_argument_value not in expected_argument_type:
                            error_msgs += f"* Found unexpected argument value {actual_argument_value} for "\
                                          f"{measure_names[measure_name]} in yaml file. Valid values are " \
                                f"{expected_argument_type}.\n"

                for arg, default in required_args_no_default.items():
                    error_msgs += f"* Required argument {arg} for measure {measure_name} wasn't supplied. " \
                        f"There is no default for this argument.\n"

                for arg, default in required_args_with_default.items():
                    warning_msgs += f"* Required argument {arg} for measure {measure_name} wasn't supplied. " \
                                    f"Using default value: {default}. \n"

            elif measure_name in ['ApplyUpgrade']:
                # For ApplyUpgrade measure, verify that all the cost_multipliers used are correct
                root = BuildStockBatchBase.get_measure_xml(os.path.join(measure_path, 'measure.xml'))
                valid_multipliers = set()
                for argument in root.findall('./arguments/argument'):
                    name = argument.find('./name')
                    if name.text.endswith('_multiplier'):
                        for choice in argument.findall('./choices/choice'):
                            value = choice.find('./value')
                            value = value.text if value is not None else ''
                            valid_multipliers.add(value)
                invalid_multipliers = Counter()
                for upgrade_count, upgrade in enumerate(cfg['upgrades']):
                    for option_count, option in enumerate(upgrade['options']):
                        for cost_indx, cost_entry in enumerate(option.get('costs', [])):
                            if cost_entry['multiplier'] not in valid_multipliers:
                                invalid_multipliers[cost_entry['multiplier']] += 1

                if invalid_multipliers:
                    error_msgs += "* The following multipliers values are invalid: \n"
                    for multiplier, count in invalid_multipliers.items():
                        error_msgs += f"    '{multiplier}' - Used {count} times \n"
                    error_msgs += f"    The list of valid multipliers are {valid_multipliers}.\n"

        if warning_msgs:
            logger.warning(warning_msgs)

        if not error_msgs:
            return True
        else:
            logger.error(error_msgs)
            raise ValidationError(error_msgs)

    @staticmethod
    def validate_options_lookup(project_file):
        """
        Validates that the parameter|options specified in the project yaml file is available in the options_lookup.tsv
        """
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        param_option_dict = defaultdict(set)
        buildstock_dir = BuildStockBatchBase.get_buildstock_dir(project_file, cfg)
        options_lookup_path = f'{buildstock_dir}/resources/options_lookup.tsv'

        # fill in the param_option_dict with {'param1':['valid_option1','valid_option2' ...]} from options_lookup.tsv
        try:
            with open(options_lookup_path, 'r') as f:
                options = csv.DictReader(f, delimiter='\t')
                invalid_options_lookup_str = ''  # Holds option/parameter names with invalid characters
                for row in options:
                    for col in ['Parameter Name', 'Option Name']:
                        invalid_chars = set(row[col]).intersection(set('|&()'))
                        invalid_chars = ''.join(invalid_chars)
                        if invalid_chars:
                            invalid_options_lookup_str += f"{col}: '{row[col]}', Invalid chars: '{invalid_chars}' \n"

                    param_option_dict[row['Parameter Name']].add(row['Option Name'])
        except FileNotFoundError as err:
            logger.error(f"Options lookup file not found at: '{options_lookup_path}'")
            raise err

        invalid_option_spec_counter = Counter()
        invalid_param_counter = Counter()
        invalid_option_counter_dict = defaultdict(Counter)

        def get_errors(source_str, option_str):
            """
            Gives multiline descriptive error message if the option_str is invalid. Returns '' otherwise
            :param source_str: the descriptive location where the option_str occurs in the yaml configuration.
            :param option_str: the param|option string representing the option choice. Can be joined by either || or &&
                               to form composite string. eg. param1|option1||param2|option2
            :return: returns empty string if the param|option is valid i.e. they are found in options_lookup.tsv
                     if not returns error message, close matches, and specifies where the error occurred (source_str)
            """
            if '||' in option_str and '&&' in option_str:
                invalid_option_spec_counter[(option_str, "has both || and && (not supported)")] += 1
                return ""

            if '||' in option_str or '&&' in option_str:
                splitter = '||' if '||' in option_str else '&&'
                errors = ''
                broken_options = option_str.split(splitter)
                if broken_options[-1] == '':
                    invalid_option_spec_counter[(option_str, "has trailing 'splitter'")] += 1
                    return ""
                for broken_option_str in broken_options:
                    new_source_str = source_str + f" in composite option '{option_str}'"
                    errors += get_errors(new_source_str, broken_option_str)
                return errors

            if not option_str or '|' == option_str:
                return f"* Option name empty. {source_str}\n"

            try:
                parameter_name, option_name = option_str.split('|')
            except ValueError:
                invalid_option_spec_counter[(option_str, "has has too many or too few '|' (exactly 1 required).")] += 1
                return ""

            if parameter_name not in param_option_dict:
                close_match = difflib.get_close_matches(parameter_name, param_option_dict.keys(), 1)
                close_match = close_match[0] if close_match else ""
                invalid_param_counter[(parameter_name, close_match)] += 1
                return ""

            if not option_name or option_name not in param_option_dict[parameter_name]:
                close_match = difflib.get_close_matches(option_name, list(param_option_dict[parameter_name]), 1)
                close_match = close_match[0] if close_match else ""
                invalid_option_counter_dict[parameter_name][(option_name, close_match)] += 1
                return ""

            return ''

        def get_all_option_str(source_str, inp):
            """
            Returns a list of (source_str, option_str) tuple by recursively traversing the logic inp structure.
            Check the get_errors function for more info about source_str and option_str
            :param source_str: the descriptive location where the inp logic is found
            :param inp: A nested apply_logic structure
            :return: List of tuples of (source_str, option_str) where source_str is the location in inp where the
                    option_str is found.
            """
            if not inp:
                return []
            if type(inp) == str:
                return [(source_str, inp)]
            elif type(inp) == list:
                return sum([get_all_option_str(source_str + f", in entry {count}", entry) for count, entry
                            in enumerate(inp)], [])
            elif type(inp) == dict:
                if len(inp) > 1:
                    raise ValueError(f"{source_str} the logic is malformed.")
                source_str += f", in {list(inp.keys())[0]}"
                return sum([get_all_option_str(source_str, i) for i in inp.values()], [])

        # store all of the option_str in the project file as a list of (source_str, option_str) tuple
        source_option_str_list = []

        if 'upgrades' in cfg:
            for upgrade_count, upgrade in enumerate(cfg['upgrades']):
                upgrade_name = upgrade.get('upgrade_name', '') + f' (Upgrade Number: {upgrade_count})'
                source_str_upgrade = f"In upgrade '{upgrade_name}'"
                for option_count, option in enumerate(upgrade['options']):
                    option_name = option.get('option', '') + f' (Option Number: {option_count})'
                    source_str_option = source_str_upgrade + f", in option '{option_name}'"
                    source_option_str_list.append((source_str_option, option.get('option')))
                    if 'apply_logic' in option:
                        source_str_logic = source_str_option + ", in apply_logic"
                        source_option_str_list += get_all_option_str(source_str_logic, option['apply_logic'])

                if 'package_apply_logic' in upgrade:
                    source_str_package = source_str_upgrade + ", in package_apply_logic"
                    source_option_str_list += get_all_option_str(source_str_package, upgrade['package_apply_logic'])

        if 'downselect' in cfg:
            source_str = "In downselect"
            source_option_str_list += get_all_option_str(source_str, cfg['downselect']['logic'])

        # Gather all the errors in the option_str, if any
        error_message = ''
        for source_str, option_str in source_option_str_list:
            error_message += get_errors(source_str, option_str)

        if error_message:
            error_message = "Following option/parameter entries have problem:\n" + error_message + "\n"

        if invalid_option_spec_counter:
            error_message += "* Following option/parameter entries have problem:\n"
            for (invalid_entry, error), count in invalid_option_spec_counter.items():
                error_message += f"  '{invalid_entry}' {error} - used '{count}' times\n"

        if invalid_param_counter:
            error_message += "* Following parameters do not exist in options_lookup.tsv\n"
            for (param, close_match), count in invalid_param_counter.items():
                error_message += f"  '{param}' - used '{count}' times."
                if close_match:
                    error_message += f" Maybe you meant to use '{close_match}'.\n"
                else:
                    error_message += "\n"

        if invalid_option_counter_dict:
            "* Following options do not exist in options_lookup.tsv\n"
            for param, options_counter in invalid_option_counter_dict.items():
                for (option, close_match), count in options_counter.items():
                    error_message += f"For param '{param}', invalid option '{option}' - used {count} times."
                    if close_match:
                        error_message += f" Maybe you meant to use '{close_match}'.\n"
                    else:
                        error_message += "\n"

        if invalid_options_lookup_str:
            error_message = "Following option/parameter names(s) have invalid characters in the options_lookup.tsv\n" +\
                            invalid_options_lookup_str + "*"*80 + "\n" + error_message

        if not error_message:
            return True
        else:
            logger.error(error_message)
            raise ValueError(error_message)

    @staticmethod
    def validate_measure_references(project_file):
        """
        Validates that the measures specified in the project yaml file are
        referenced in the options_lookup.tsv
        """
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        measure_dirs = set()
        buildstock_dir = BuildStockBatchBase.get_buildstock_dir(project_file, cfg)
        options_lookup_path = f'{buildstock_dir}/resources/options_lookup.tsv'

        # fill in the param_option_dict with {'param1':['valid_option1','valid_option2' ...]} from options_lookup.tsv
        try:
            with open(options_lookup_path, 'r') as f:
                options = csv.DictReader(f, delimiter='\t')
                for row in options:
                    if row['Measure Dir']:
                        measure_dirs.add(row['Measure Dir'])
        except FileNotFoundError as err:
            logger.error(f"Options lookup file not found at: '{options_lookup_path}'")
            raise err

        def get_errors(source_str, measure_str):
            """
            Gives multiline descriptive error message if the measure_str is invalid. Returns '' otherwise
            :param source_str: the descriptive location where the measure_str occurs in the yaml configuration.
            :param measure_str: the string containing a reference to a measure directory
            :return: returns empty string if the measure_str is a valid measure
                     directory name as referenced in the options_lookup.tsv.
                     if not returns error message, close matches, and specifies
                     where the error occurred (source_str).
            """
            if measure_str not in measure_dirs:
                closest = difflib.get_close_matches(measure_str, list(measure_dirs))
                return f"Measure directory {measure_str} not found. Closest matches: {closest}" \
                    f" {source_str}\n"
            return ''

        source_measures_str_list = []

        if 'measures_to_ignore' in cfg['baseline']:
            source_str = "In baseline 'measures_to_ignore'"
            for measure_str in cfg['baseline']['measures_to_ignore']:
                source_measures_str_list.append((source_str, measure_str))

        error_message = ''
        for source_str, measure_str in source_measures_str_list:
            error_message += get_errors(source_str, measure_str)

        if not error_message:
            return True
        else:
            error_message = 'Measure name(s)/directory(ies) is(are) invalid. \n' + error_message
            logger.error(error_message)
            raise ValueError(error_message)

    @staticmethod
    def validate_reference_scenario(project_file):
        """
        Checks if the reference_scenario mentioned in an upgrade points to a valid upgrade
        """
        cfg = BuildStockBatchBase.get_project_configuration(project_file)

        # collect all upgrade_names
        upgrade_names = set()
        for upgrade_count, upgrade in enumerate(cfg.get('upgrades', [])):
            upgrade_names.add(upgrade.get('upgrade_name', ''))

        warning_string = ""
        # check if the reference_scenario matches with any upgrade_names
        for upgrade_count, upgrade in enumerate(cfg.get('upgrades', [])):
            if 'reference_scenario' in upgrade:
                if upgrade['reference_scenario'] not in upgrade_names:
                    warning_string += f"* In Upgrade '{upgrade.get('upgrade_name', '')}', reference_scenario: " \
                        f"'{upgrade['reference_scenario']}' does not match any existing upgrade names \n"
                elif upgrade['reference_scenario'] == upgrade.get('upgrade_name', ''):
                    warning_string += f"* In Upgrade '{upgrade.get('upgrade_name', '')}', reference_scenario: " \
                        f"'{upgrade['reference_scenario']}' points to the same upgrade \n"

        if warning_string:
            logger.warning(warning_string)

        return True  # Only print the warning, but always pass the validation

    def get_dask_client(self):
        return Client()

    def process_results(self, skip_combine=False, force_upload=False):
        self.get_dask_client()  # noqa: F841

        do_timeseries = 'timeseries_csv_export' in self.cfg.keys()

        fs = LocalFileSystem()
        if not skip_combine:
            postprocessing.combine_results(fs, self.results_dir, self.cfg, do_timeseries=do_timeseries)

        aws_conf = self.cfg.get('postprocessing', {}).get('aws', {})
        if 's3' in aws_conf or force_upload:
            s3_bucket, s3_prefix = postprocessing.upload_results(aws_conf, self.output_dir, self.results_dir)
            if 'athena' in aws_conf:
                postprocessing.create_athena_tables(aws_conf, os.path.basename(self.output_dir), s3_bucket, s3_prefix)

        postprocessing.remove_intermediate_files(fs, self.results_dir)
