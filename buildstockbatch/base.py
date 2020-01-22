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
from collections import defaultdict
import xml.etree.ElementTree as ET

from buildstockbatch.__version__ import __schema_version__
from .workflow_generator import ResidentialDefaultWorkflowGenerator, CommercialDefaultWorkflowGenerator
from .postprocessing import combine_results, upload_results, create_athena_tables, write_dataframe_as_parquet

logger = logging.getLogger(__name__)


class SimulationExists(Exception):
    pass


class ValidationError(Exception):
    pass


class BuildStockBatchBase(object):
    DEFAULT_OS_VERSION = '2.8.1'
    DEFAULT_OS_SHA = '88b69707f1'
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

        # Call property to create directory and copy weather files there
        _ = self.weather_dir  # noqa: F841
        # Load in OS_VERSION and OS_SHA arguments if they exist in the YAML,
        # otherwise use defaults specified here.
        self.os_version = self.cfg.get('os_version', self.DEFAULT_OS_VERSION)
        self.os_sha = self.cfg.get('os_sha', self.DEFAULT_OS_SHA)
        logger.debug(f"Using OpenStudio version: {self.os_version} with SHA: {self.os_sha}")

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
        raise NotImplementedError

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

    @property
    def skip_baseline_sims(self):
        baseline_skip = self.cfg['baseline'].get('skip_sims', False)
        return baseline_skip

    def run_sampling(self, n_datapoints=None):
        if n_datapoints is None:
            n_datapoints = self.cfg['baseline']['n_datapoints']
        if 'buildstock_csv' in self.cfg['baseline']:
            buildstock_csv = self.path_rel_to_projectfile(self.cfg['baseline']['buildstock_csv'])
            destination_filename = self.sampler.csv_path
            if destination_filename != buildstock_csv:
                if os.path.exists(destination_filename):
                    logger.info("Removing {!r} before copying {!r} to that location."
                                .format(destination_filename, buildstock_csv))
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

        fs = LocalFileSystem()

        # Convert the timeseries data to parquet
        timeseries_filepath = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
        if os.path.isfile(timeseries_filepath):
            tsdf = pd.read_csv(timeseries_filepath, parse_dates=['Time'])
            write_dataframe_as_parquet(tsdf, fs, os.path.splitext(timeseries_filepath)[0] + '.parquet')

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
        assert(BuildStockBatchBase.validate_reference_scenario(project_file))
        assert(BuildStockBatchBase.validate_measures_and_arguments(project_file))
        assert(BuildStockBatchBase.validate_options_lookup(project_file))
        assert(BuildStockBatchBase.validate_measure_references(project_file))
        assert(BuildStockBatchBase.validate_reference_scenario(project_file))

        logger.info('Base Validation Successful')
        return True

    @staticmethod
    def get_project_configuration(project_file):
        try:
            with open(project_file) as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
        except FileNotFoundError as err:
            logger.error(f'Failed to load input yaml for validation')
            raise err
        return cfg

    @staticmethod
    def get_buildstock_dir(project_file, cfg):
        buildstock_dir = cfg["buildstock_directory"]
        if os.path.isabs(buildstock_dir):
            return os.path.abspath(buildstock_dir)
        else:
            return os.path.abspath(os.path.join(os.path.dirname(project_file), buildstock_dir))

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
    def validate_xor_schema_keys(project_file):
        cfg = BuildStockBatchBase.get_project_configuration(project_file)
        major, minor = cfg.get('version', __schema_version__).split('.')
        if int(major) >= 0:
            if int(minor) >= 0:
                if ('weather_files_url' in cfg.keys()) is \
                   ('weather_files_path' in cfg.keys()):
                    raise ValidationError('Both/neither weather_files_url and weather_files_path found in yaml root')
                if ('n_datapoints' in cfg['baseline'].keys()) is \
                   ('buildstock_csv' in cfg['baseline'].keys()):
                    raise ValidationError('Both/neither n_datapoints and buildstock_csv found in yaml baseline key')
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
                        'SimulationOutputReport': 'simulation_output_report',
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
            if measure_name in ['ResidentialSimulationControls', 'TimeseriesCSVExport']:
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

                for upgrade_count, upgrade in enumerate(cfg['upgrades']):
                    upgrade_name = upgrade.get('upgrade_name', '') + f' (Upgrade Number: {upgrade_count})'
                    for option_count, option in enumerate(upgrade['options']):
                        option_name = option.get('option', '') + f' (Option Number: {option_count})'
                        for cost_indx, cost_entry in enumerate(option.get('costs', [])):
                            if cost_entry['multiplier'] not in valid_multipliers:
                                error_msgs += f"* Invalid multiplier '{cost_entry['multiplier']}' specified "\
                                              f"in option {option_name} in upgrade {upgrade_name}. The list of valid "\
                                              f"multipliers are {valid_multipliers}.\n"

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
        Validates that the parameter|options specified in the project yaml file is avaliable in the options_lookup.tsv
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
                return f"* Option specification '{option_str}' has both || and &&, which is not supported. " \
                    f"{source_str}\n"

            if '||' in option_str or '&&' in option_str:
                splitter = '||' if '||' in option_str else '&&'
                errors = ''
                broken_options = option_str.split(splitter)
                if broken_options[-1] == '':
                    return f"* Option spec '{option_str}' has a trailing '{splitter}'. {source_str}\n"
                for broken_option_str in broken_options:
                    new_source_str = source_str + f" in composite option '{option_str}'"
                    errors += get_errors(new_source_str, broken_option_str)
                return errors

            if not option_str or '|' == option_str:
                return f"* Option name empty. {source_str}\n"

            try:
                parameter_name, option_name = option_str.split('|')
            except ValueError:
                return f"* Option specification '{option_str}' has too many or too few '|' (exactly 1 required)." \
                    f" {source_str}\n"

            if parameter_name not in param_option_dict:
                error_str = f"* Parameter name '{parameter_name}' does not exist in options_lookup. \n"
                close_match = difflib.get_close_matches(parameter_name, param_option_dict.keys(), 1)
                if close_match:
                    error_str += f"Maybe you meant to type '{close_match[0]}'. \n"
                error_str += f"{source_str}\n"
                return error_str

            if not option_name or option_name not in param_option_dict[parameter_name]:
                error_str = f"* Option name '{option_name}' does not exist in options_lookup " \
                    f"for parameter '{parameter_name}'. \n"
                close_match = difflib.get_close_matches(option_name, list(param_option_dict[parameter_name]), 1)
                if close_match:
                    error_str += f"Maybe you meant to type '{close_match[0]}'. \n"
                error_str += f"{source_str}\n"
                return error_str

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
            source_str = f"In downselect"
            source_option_str_list += get_all_option_str(source_str, cfg['downselect']['logic'])

        # Gather all the errors in the option_str, if any
        error_message = ''
        for source_str, option_str in source_option_str_list:
            error_message += get_errors(source_str, option_str)

        if error_message:
            error_message = "Following option/parameter name(s) in the yaml file is(are) invalid. \n" + error_message

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
            source_str = f"In baseline 'measures_to_ignore'"
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

        if self.cfg.get('timeseries_csv_export', {}):
            skip_timeseries = False
        else:
            skip_timeseries = True

        aggregate_ts = self.cfg.get('postprocessing', {}).get('aggregate_timeseries', False)

        reporting_measures = self.cfg.get('reporting_measures', [])

        fs = LocalFileSystem()

        if not skip_combine:
            combine_results(fs, self.results_dir, self.cfg, skip_timeseries=skip_timeseries,
                            aggregate_timeseries=aggregate_ts, reporting_measures=reporting_measures)

        aws_conf = self.cfg.get('postprocessing', {}).get('aws', {})
        if 's3' in aws_conf or force_upload:
            s3_bucket, s3_prefix = upload_results(aws_conf, self.output_dir, self.results_dir)
            if 'athena' in aws_conf:
                create_athena_tables(aws_conf, os.path.basename(self.output_dir), s3_bucket, s3_prefix)
