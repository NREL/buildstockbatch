# -*- coding: utf-8 -*-

"""
buildstockbatch.base
~~~~~~~~~~~~~~~
This is the base class mixed into the deployment specific classes (i.e. eagle, local)

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from dask.distributed import Client
import difflib
from fsspec.implementations.local import LocalFileSystem
import logging
from lxml import objectify
import os
import numpy as np
import re
import requests
import shutil
import subprocess
import tempfile
import yamale
import zipfile
import csv
from collections import defaultdict, Counter
import pprint

from buildstockbatch.__version__ import __schema_version__
from buildstockbatch import (
    sampler,
    workflow_generator,
    postprocessing
)
from buildstockbatch.exc import SimulationExists, ValidationError
from buildstockbatch.utils import path_rel_to_file, get_project_configuration, read_csv
from buildstockbatch.__version__ import __version__ as bsb_version

logger = logging.getLogger(__name__)


class BuildStockBatchBase(object):

    # http://openstudio-builds.s3-website-us-east-1.amazonaws.com
    DEFAULT_OS_VERSION = '3.6.1'
    DEFAULT_OS_SHA = 'bb9481519e'
    CONTAINER_RUNTIME = None
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
        self.cfg = get_project_configuration(project_filename)

        self.buildstock_dir = self.cfg['buildstock_directory']
        if not os.path.isdir(self.buildstock_dir):
            raise FileNotFoundError(f'buildstock_directory = {self.buildstock_dir} is not a directory.')
        self.project_dir = os.path.join(self.buildstock_dir, self.cfg['project_directory'])
        if not os.path.isdir(self.project_dir):
            raise FileNotFoundError(f'project_directory = {self.project_dir} is not a directory.')

        # Load in OS_VERSION and OS_SHA arguments if they exist in the YAML,
        # otherwise use defaults specified here.
        self.os_version = self.cfg.get('os_version', self.DEFAULT_OS_VERSION)
        self.os_sha = self.cfg.get('os_sha', self.DEFAULT_OS_SHA)
        logger.debug(f"Using OpenStudio version: {self.os_version} with SHA: {self.os_sha}")

    @staticmethod
    def get_sampler_class(sampler_name):
        sampler_class_name = ''.join(x.capitalize() for x in sampler_name.strip().split('_')) + 'Sampler'
        return getattr(sampler, sampler_class_name)

    @staticmethod
    def get_workflow_generator_class(workflow_generator_name):
        workflow_generator_class_name = \
            ''.join(x.capitalize() for x in workflow_generator_name.strip().split('_')) + 'WorkflowGenerator'
        return getattr(workflow_generator, workflow_generator_class_name)

    @property
    def sampler(self):
        # Select a sampler
        Sampler = self.get_sampler_class(self.cfg['sampler']['type'])
        return Sampler(self, **self.cfg['sampler'].get('args', {}))

    @staticmethod
    def openstudio_exe():
        return os.environ.get("OPENSTUDIO_EXE", "openstudio")

    def path_rel_to_projectfile(self, x):
        return path_rel_to_file(self.project_filename, x)

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
            with tempfile.TemporaryFile(dir=os.environ.get('LOCAL_SCRATCH')) as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.seek(0)
                with zipfile.ZipFile(f, 'r') as zf:
                    logger.debug('Extracting weather files to: {}'.format(self.weather_dir))
                    zf.extractall(self.weather_dir)

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

    @classmethod
    def get_reporting_measures(cls, cfg):
        WorkflowGenerator = cls.get_workflow_generator_class(cfg['workflow_generator']['type'])
        wg = WorkflowGenerator(cfg, 1)  # Number of datapoints doesn't really matter here
        return wg.reporting_measures()

    def run_batch(self):
        raise NotImplementedError

    @classmethod
    def create_osw(cls, cfg, n_datapoints, *args, **kwargs):
        WorkflowGenerator = cls.get_workflow_generator_class(cfg['workflow_generator']['type'])
        osw_generator = WorkflowGenerator(cfg, n_datapoints)
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
        timeseries_filepath = os.path.join(sim_dir, 'run', 'results_timeseries.csv')
        # FIXME: Allowing both names here for compatibility. Should consolidate on one timeseries filename.
        if os.path.isfile(timeseries_filepath):
            units_dict = read_csv(timeseries_filepath, nrows=1).transpose().to_dict()[0]
            skiprows = [1]
        else:
            timeseries_filepath = os.path.join(sim_dir, 'run', 'enduse_timeseries.csv')
            units_dict = {}
            skiprows = []

        schedules_filepath = ''
        if os.path.isdir(os.path.join(sim_dir, 'generated_files')):
            for file in os.listdir(os.path.join(sim_dir, 'generated_files')):
                if re.match(r".*schedules.*\.csv", file):
                    schedules_filepath = os.path.join(sim_dir, 'generated_files', file)

        if os.path.isfile(timeseries_filepath):
            # Find the time columns present in the enduse_timeseries file
            possible_time_cols = ['time', 'Time', 'TimeDST', 'TimeUTC']
            cols = read_csv(timeseries_filepath, index_col=False, nrows=0).columns.tolist()
            actual_time_cols = [c for c in cols if c in possible_time_cols]
            if not actual_time_cols:
                logger.error(f'Did not find any time column ({possible_time_cols}) in {timeseries_filepath}.')
                raise RuntimeError(f'Did not find any time column ({possible_time_cols}) in {timeseries_filepath}.')

            tsdf = read_csv(timeseries_filepath, parse_dates=actual_time_cols, skiprows=skiprows)
            if os.path.isfile(schedules_filepath):
                schedules = read_csv(schedules_filepath, dtype=np.float64)
                schedules.rename(columns=lambda x: f'schedules_{x}', inplace=True)
                schedules['TimeDST'] = tsdf['Time']
                tsdf = tsdf.merge(schedules, how='left', on='TimeDST')

            def get_clean_column_name(x):
                """"
                Will rename column names like End Use: Natural Gas: Range/Oven to
                end_use__natural_gas__range_oven__kbtu to play nice with Athena
                """
                unit = units_dict.get(x)  # missing units (e.g. for time) gets nan
                unit = unit if isinstance(unit, str) else ''
                sepecial_characters = [':', ' ', '/']
                for char in sepecial_characters:
                    x = x.replace(char, '_')
                x = x + "__" + unit if unit else x
                return x.lower()

            tsdf.rename(columns=get_clean_column_name, inplace=True)
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

    @classmethod
    def validate_project(cls, project_file):
        assert cls.validate_project_schema(project_file)
        assert cls.validate_sampler(project_file)
        assert cls.validate_workflow_generator(project_file)
        assert cls.validate_misc_constraints(project_file)
        assert cls.validate_xor_nor_schema_keys(project_file)
        assert cls.validate_reference_scenario(project_file)
        assert cls.validate_options_lookup(project_file)
        assert cls.validate_logic(project_file)
        assert cls.validate_measure_references(project_file)
        assert cls.validate_postprocessing_spec(project_file)
        assert cls.validate_resstock_or_comstock_version(project_file)
        assert cls.validate_openstudio_version(project_file)
        assert cls.validate_number_of_options(project_file)
        logger.info('Base Validation Successful')
        return True

    @staticmethod
    def get_buildstock_dir(project_file, cfg):
        buildstock_dir = cfg["buildstock_directory"]
        if os.path.isabs(buildstock_dir):
            return os.path.abspath(buildstock_dir)
        else:
            return os.path.abspath(os.path.join(os.path.dirname(project_file), buildstock_dir))

    @classmethod
    def validate_openstudio_path(cls, project_file):
        cfg = get_project_configuration(project_file)
        os_version = cfg.get('os_version', cls.DEFAULT_OS_VERSION)
        os_sha = cfg.get('os_sha', cls.DEFAULT_OS_SHA)
        try:
            proc_out = subprocess.run(
                [cls.openstudio_exe(), "openstudio_version"],
                capture_output=True,
                text=True
            )
        except FileNotFoundError:
            raise ValidationError(f"Cannot find openstudio at `{cls.openstudio_exe()}`")
        if proc_out.returncode != 0:
            raise ValidationError(f"OpenStudio failed with the following error {proc_out.stderr}")
        actual_os_version, actual_os_sha = proc_out.stdout.strip().split("+")
        if os_version != actual_os_version:
            raise ValidationError(f"OpenStudio version is {actual_os_version}, expected is {os_version}")
        if os_sha != actual_os_sha:
            raise ValidationError(
                f"OpenStudio version is correct at {os_version}, but the shas don't match. "
                f"Got {actual_os_sha}, expected {os_sha}"
            )
        return True

    @staticmethod
    def validate_sampler(project_file):
        cfg = get_project_configuration(project_file)
        sampler_name = cfg['sampler']['type']
        try:
            Sampler = BuildStockBatchBase.get_sampler_class(sampler_name)
        except AttributeError:
            raise ValidationError(f'Sampler class `{sampler_name}` is not available.')
        args = cfg['sampler']['args']
        Sampler.validate_args(project_file, **args)
        if issubclass(Sampler, sampler.PrecomputedSampler):
            sample_file = cfg['sampler']['args']['sample_file']
            if not os.path.isabs(sample_file):
                sample_file = os.path.join(os.path.dirname(project_file), sample_file)
            else:
                sample_file = os.path.abspath(sample_file)
            buildstock_df = read_csv(sample_file, dtype=str)
            return BuildStockBatchBase.validate_buildstock_csv(project_file, buildstock_df)
        return True

    @staticmethod
    def validate_buildstock_csv(project_file, buildstock_df):
        param_option_dict, _ = BuildStockBatchBase.get_param_option_dict(project_file)
        # verify that all the Columns in buildstock_df only have values available in param_option_dict
        # param_option_dict has format: {column_name: [valid_option1, valid_option2, ...], ...}
        errors = []
        for column in buildstock_df.columns:
            if column in {'Building'}:
                continue
            if column not in param_option_dict:
                errors.append(f'Column {column} in buildstock_csv is not available in options_lookup.tsv')
                continue
            if "*" in param_option_dict[column]:
                continue  # skip validating options when wildcard is present
            for option in buildstock_df[column].unique():
                if option not in param_option_dict[column]:
                    errors.append(f'Option {option} in column {column} of buildstock_csv is not available '
                                  'in options_lookup.tsv')
        if errors:
            raise ValidationError('\n'.join(errors))

        return True

    @classmethod
    def validate_workflow_generator(cls, project_file):
        cfg = get_project_configuration(project_file)
        WorkflowGenerator = cls.get_workflow_generator_class(cfg['workflow_generator']['type'])
        return WorkflowGenerator.validate(cfg)

    @staticmethod
    def validate_project_schema(project_file):
        cfg = get_project_configuration(project_file)
        schema_version = cfg.get('schema_version')
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
        cfg = get_project_configuration(project_file)  # noqa F841

        return True

    @staticmethod
    def validate_postprocessing_spec(project_file):
        cfg = get_project_configuration(project_file)  # noqa F841
        param_option_dict, _ = BuildStockBatchBase.get_param_option_dict(project_file)
        partition_cols = cfg.get('postprocessing', {}).get("partition_columns", [])
        invalid_cols = [c for c in partition_cols if c not in param_option_dict.keys()]
        if invalid_cols:
            raise ValidationError(f"The following partition columns are not valid: {invalid_cols}")
        return True

    @staticmethod
    def validate_xor_nor_schema_keys(project_file):
        cfg = get_project_configuration(project_file)
        major, minor = cfg.get('version', __schema_version__).split('.')
        if int(major) >= 0:
            if int(minor) >= 0:
                # xor
                if ('weather_files_url' in cfg.keys()) is \
                   ('weather_files_path' in cfg.keys()):
                    raise ValidationError('Both/neither weather_files_url and weather_files_path found in yaml root')

        return True

    @staticmethod
    def get_param_option_dict(project_file):
        cfg = get_project_configuration(project_file)
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
                    param_name, opt_name = row['Parameter Name'], row['Option Name']
                    param_option_dict[row['Parameter Name']].add(row['Option Name'])
                    if opt_name == '*' and row['Measure Dir']:
                        invalid_options_lookup_str += f"{param_name}: '*' cannot pass arguments to measure.\n"
                    if "*" in param_option_dict[param_name] and len(param_option_dict[param_name]) > 1:
                        invalid_options_lookup_str += f"{param_name}: '*' cannot be mixed with other options\n"
        except FileNotFoundError as err:
            logger.error(f"Options lookup file not found at: '{options_lookup_path}'")
            raise err
        return param_option_dict, invalid_options_lookup_str

    @staticmethod
    def validate_options_lookup(project_file):
        """
        Validates that the parameter|options specified in the project yaml file is available in the options_lookup.tsv
        """
        cfg = get_project_configuration(project_file)
        param_option_dict, invalid_options_lookup_str = BuildStockBatchBase.get_param_option_dict(project_file)
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
                    raise ValidationError(f"{source_str} the logic is malformed. Dict can't have more than one entry")
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

        #  TODO: refactor this into Sampler.validate_args
        if 'downselect' in cfg or "downselect" in cfg.get('sampler', {}).get('type'):
            source_str = "In downselect"
            logic = cfg['downselect']['logic'] if 'downselect' in cfg else cfg['sampler']['args']['logic']
            source_option_str_list += get_all_option_str(source_str, logic)

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
            raise ValidationError(error_message)

    @staticmethod
    def validate_logic(project_file):
        """
        Validates that the apply logic has basic consistency.
        Currently checks the following rules:
        1. A 'and' block or a 'not' block doesn't contain two identical options entry. For example, the following is an
        invalid block because no building can have both of those characteristics.
        not:
           - HVAC Heating Efficiency|ASHP, SEER 10, 6.2 HSPF
           - HVAC Heating Efficiency|ASHP, SEER 13, 7.7 HSPF
        """
        cfg = get_project_configuration(project_file)

        printer = pprint.PrettyPrinter()

        def get_option(element):
            return element.split('|')[0] if isinstance(element, str) else None

        def get_logic_problems(logic, parent=None):
            if isinstance(logic, list):
                all_options = [opt for el in logic if (opt := get_option(el))]
                problems = []
                if parent in ['not', 'and', None, '&&']:
                    for opt, count in Counter(all_options).items():
                        if count > 1:
                            parent_name = parent or 'and'
                            problem_text = f"Option '{opt}' occurs {count} times in a '{parent_name}' block. "\
                                f"It should occur at max one times. This is the block:\n{printer.pformat(logic)}"
                            if parent is None:
                                problem_text += "\nRemember a list without a parent is considered an 'and' block."
                            problems.append(problem_text)
                for el in logic:
                    problems += get_logic_problems(el)
                return problems
            elif isinstance(logic, dict):
                assert len(logic) == 1
                for key, val in logic.items():
                    if key not in ['or', 'and', 'not']:
                        raise ValidationError(f"Invalid key {key}. Only 'or', 'and' and 'not' is allowed.")
                    return get_logic_problems(val, parent=key)
            elif isinstance(logic, str):
                if '&&' not in logic:
                    return []
                entries = logic.split('&&')
                return get_logic_problems(entries, parent="&&")
            else:
                raise ValidationError(f"Invalid logic element {logic} with type {type(logic)}")

        all_problems = []
        if 'upgrades' in cfg:
            for upgrade_count, upgrade in enumerate(cfg['upgrades']):
                upgrade_name = upgrade.get('upgrade_name', '')
                source_str_upgrade = f"upgrade '{upgrade_name}' (Upgrade Number:{upgrade_count})"
                for option_count, option in enumerate(upgrade['options']):
                    option_name = option.get('option', '')
                    source_str_option = source_str_upgrade + f", option '{option_name}' (Option Number:{option_count})"
                    if 'apply_logic' in option:
                        if problems := get_logic_problems(option['apply_logic']):
                            all_problems.append((source_str_option, problems, option['apply_logic']))

                if 'package_apply_logic' in upgrade:
                    source_str_package = source_str_upgrade + ", in package_apply_logic"
                    if problems := get_logic_problems(upgrade['package_apply_logic']):
                        all_problems.append((source_str_package, problems, upgrade['package_apply_logic']))

        #  TODO: refactor this into Sampler.validate_args
        if 'downselect' in cfg or "downselect" in cfg.get('sampler', {}).get('type'):
            source_str = "in downselect logic"
            logic = cfg['downselect']['logic'] if 'downselect' in cfg else cfg['sampler']['args']['logic']
            if problems := get_logic_problems(logic):
                all_problems.append((source_str, problems, logic))

        if all_problems:
            error_str = ''
            for location, problems, logic in all_problems:
                error_str += f"There are following problems in {location} with this logic\n{printer.pformat(logic)}\n"
                problem_str = "\n".join(problems)
                error_str += f"The problems are:\n{problem_str}\n"
            raise ValidationError(error_str)
        else:
            return True

    @staticmethod
    def validate_measure_references(project_file):
        """
        Validates that the measures specified in the project yaml file are
        referenced in the options_lookup.tsv
        """
        cfg = get_project_configuration(project_file)
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
            raise ValidationError(error_message)

    @staticmethod
    def validate_reference_scenario(project_file):
        """
        Checks if the reference_scenario mentioned in an upgrade points to a valid upgrade
        """
        cfg = get_project_configuration(project_file)

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

    @staticmethod
    def validate_resstock_or_comstock_version(project_file):
        """
        Checks the minimum required version of BuildStockBatch against the version being used
        """
        cfg = get_project_configuration(project_file)

        buildstock_rb = os.path.join(cfg['buildstock_directory'], 'resources/buildstock.rb')
        if os.path.exists(buildstock_rb):
            versions = {}
            with open(buildstock_rb, 'r') as f:
                for line in f:
                    line = line.strip()
                    for tool in ['ResStock_Version', 'ComStock_Version', 'BuildStockBatch_Version']:
                        if line.startswith(tool):
                            lhs, rhs = line.split('=')
                            version, _ = rhs.split('#')
                            versions[tool] = eval(version.strip())
            BuildStockBatch_Version = versions['BuildStockBatch_Version']
            if bsb_version < BuildStockBatch_Version:
                if 'ResStock_Version' in versions.keys():
                    stock_version = versions['ResStock_Version']
                elif 'ComStock_Version' in versions.keys():
                    stock_version = versions['ComStock_Version']
                else:
                    stock_version = 'Unknown'
                val_err = f"BuildStockBatch version {BuildStockBatch_Version} or above is required" \
                    f" for ResStock or ComStock version {stock_version}. Found {bsb_version}"
                raise ValidationError(val_err)

        return True

    @staticmethod
    def validate_number_of_options(project_file):
        """Checks that there aren't more options than are allowed in the ApplyUpgrade measure.

        :param project_file: path to project file
        :type project_file: str
        :raises ValidationError: if there are more options defined than there are in the measure.
        :return: whether validation passes or not
        :rtype: bool
        """
        cfg = get_project_configuration(project_file)
        measure_xml_filename = os.path.join(
            cfg["buildstock_directory"], "measures", "ApplyUpgrade", "measure.xml"
        )
        if os.path.exists(measure_xml_filename):
            measure_xml_tree = objectify.parse(measure_xml_filename)
            measure_xml = measure_xml_tree.getroot()
            n_options_in_measure = 0
            n_costs_per_option_in_measure = 0
            for argument in measure_xml.arguments.argument:
                m_option = re.match(r"^option_(\d+)$", str(argument.name))
                if m_option:
                    option_number = int(m_option.group(1))
                    n_options_in_measure = max(option_number, n_options_in_measure)
                m_costs = re.match(r"^option_(\d+)_cost_(\d+)_value", str(argument.name))
                if m_costs:
                    cost_number = int(m_costs.group(2))
                    n_costs_per_option_in_measure = max(cost_number, n_costs_per_option_in_measure)
            n_options_in_cfg = 0
            n_costs_in_cfg = 0
            for upgrade in cfg.get("upgrades", []):
                options = upgrade.get("options", [])
                n_options = len(options)
                n_costs = max(len(option.get("costs", [])) for option in options)
                n_options_in_cfg = max(n_options, n_options_in_cfg)
                n_costs_in_cfg = max(n_costs, n_costs_in_cfg)
            err_msgs = []
            if n_options_in_cfg > n_options_in_measure:
                err_msgs.append(
                    f"There are {n_options_in_cfg} options in an upgrade in your project file and only "
                    f"{n_options_in_measure} in the ApplyUpgrade measure."
                )
            if n_costs_in_cfg > n_costs_per_option_in_measure:
                err_msgs.append(
                    f"There are {n_costs_in_cfg} costs on an option in your project file and only "
                    f"{n_costs_per_option_in_measure} in the ApplyUpgrade measure."
                )
            if err_msgs:
                err_msgs.append(
                    "See https://github.com/NREL/buildstockbatch/wiki/Adding-Options-to-the-ApplyUpgrade-measure"
                )
                raise ValidationError("\n".join(err_msgs))

        return True

    @staticmethod
    def validate_openstudio_version(project_file):
        """
        Checks the required version of OpenStudio against the version being used
        """
        cfg = get_project_configuration(project_file)

        os_version = cfg.get('os_version', BuildStockBatchBase.DEFAULT_OS_VERSION)
        version_path = 'resources/hpxml-measures/HPXMLtoOpenStudio/resources/version.rb'
        version_rb = os.path.join(cfg['buildstock_directory'], version_path)
        if os.path.exists(version_rb):
            versions = {}
            with open(version_rb, 'r') as f:
                for line in f:
                    line = line.strip()
                    for tool in ['OS_HPXML_Version', 'OS_Version']:
                        if line.startswith(tool):
                            lhs, rhs = line.split('=')
                            version, _ = rhs.split('#')
                            versions[tool] = eval(version.strip())
            OS_HPXML_Version = versions['OS_HPXML_Version']
            OS_Version = versions['OS_Version']
            if not os_version.startswith(OS_Version):
                val_err = f"OS version {OS_Version} is required" \
                    f" for OS-HPXML version {OS_HPXML_Version}. Found {os_version}"
                raise ValidationError(val_err)

        return True

    def get_dask_client(self):
        return Client()

    def process_results(self, skip_combine=False, force_upload=False):
        self.get_dask_client()  # noqa: F841

        if self.cfg['workflow_generator']['type'] == 'residential_hpxml':
            if 'simulation_output_report' in self.cfg['workflow_generator']['args'].keys():
                if 'timeseries_frequency' in self.cfg['workflow_generator']['args']['simulation_output_report'].keys():
                    do_timeseries = \
                        (self.cfg['workflow_generator']['args']['simulation_output_report']['timeseries_frequency'] !=
                            'none')
        else:
            do_timeseries = 'timeseries_csv_export' in self.cfg['workflow_generator']['args'].keys()

        fs = LocalFileSystem()
        if not skip_combine:
            postprocessing.combine_results(fs, self.results_dir, self.cfg, do_timeseries=do_timeseries)

        aws_conf = self.cfg.get('postprocessing', {}).get('aws', {})
        if 's3' in aws_conf or force_upload:
            s3_bucket, s3_prefix = postprocessing.upload_results(
                aws_conf, self.output_dir, self.results_dir, self.sampler.csv_path
            )
            if 'athena' in aws_conf:
                postprocessing.create_athena_tables(aws_conf, os.path.basename(self.output_dir), s3_bucket, s3_prefix)

        keep_individual_timeseries = self.cfg.get('postprocessing', {}).get('keep_individual_timeseries', False)
        postprocessing.remove_intermediate_files(fs, self.results_dir, keep_individual_timeseries)
