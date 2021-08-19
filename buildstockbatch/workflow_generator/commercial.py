# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.commercial
~~~~~~~~~~~~~~~
This object contains the commercial classes for generating OSW files from individual samples

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import datetime as dt
import json
import logging
import os
import re
from xml.etree import ElementTree
import yamale

from .base import WorkflowGeneratorBase
from buildstockbatch.exc import ValidationError

logger = logging.getLogger(__name__)


def get_measure_xml(xml_path):
    tree = ElementTree.parse(xml_path)
    root = tree.getroot()
    return root


class CommercialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    @classmethod
    def validate(cls, cfg):
        """Validate arguments

        :param cfg: project configuration
        :type cfg: dict
        """
        schema_yml = """
        measures: list(include('measure-spec'), required=False)
        reporting_measures: list(include('measure-spec'), required=False)
        timeseries_csv_export: map(required=False)
        ---
        measure-spec:
            measure_dir_name: str(required=True)
            arguments: map(required=False)
        """
        workflow_generator_args = cfg['workflow_generator']['args']
        schema_yml = re.sub(r'^ {8}', '', schema_yml, flags=re.MULTILINE)
        schema = yamale.make_schema(content=schema_yml, parser='ruamel')
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser='ruamel')
        return yamale.validate(schema, data, strict=True)

    def reporting_measures(self):
        """Return a list of reporting measures to include in outputs"""
        workflow_args = self.cfg['workflow_generator'].get('args', {})

        # reporting_measures needs to return the ClassName in measure.rb, but
        # measure_dir_name in ComStock doesn't always match the ClassName
        buildstock_dir = self.cfg['buildstock_directory']
        measures_dir = os.path.join(buildstock_dir, 'measures')
        measure_class_names = []
        for m in workflow_args.get('reporting_measures', []):
            measure_dir_name = m['measure_dir_name']
            measure_path = os.path.join(measures_dir, measure_dir_name)
            root = get_measure_xml(os.path.join(measure_path, 'measure.xml'))
            measure_class_name = root.find('./class_name').text
            # Don't include OpenStudioResults, it has too many registerValues for ComStock
            if measure_class_name == 'OpenStudioResults':
                continue
            measure_class_names.append(measure_class_name)

        return measure_class_names

    @staticmethod
    def validate_measures_and_arguments(cfg):

        buildstock_dir = cfg["buildstock_directory"]
        measures_dir = os.path.join(buildstock_dir, 'measures')
        type_map = {'Integer': int, 'Boolean': bool, 'String': str, 'Double': float}

        measure_names = {
            'BuildExistingModel': 'baseline',
            'SimulationOutputReport': 'workflow_generator.args.simulation_output',
            'ApplyUpgrade': 'upgrades',
            'TimeseriesCSVExport': 'workflow_generator.args.timeseries_csv_export'
        }

        def cfg_path_exists(cfg_path):
            if cfg_path is None:
                return False
            path_items = cfg_path.split('.')
            a = cfg
            for path_item in path_items:
                try:
                    a = a[path_item]  # noqa F841
                except KeyError:
                    return False
            return True

        def get_cfg_path(cfg_path):
            if cfg_path is None:
                return None
            path_items = cfg_path.split('.')
            a = cfg
            for path_item in path_items:
                try:
                    a = a[path_item]
                except KeyError:
                    return None
            return a

        workflow_args = cfg['workflow_generator'].get('args', {})
        if 'reporting_measures' in workflow_args.keys():
            for reporting_measure in workflow_args['reporting_measures']:
                measure_names[reporting_measure['measure_dir_name']] = 'workflow_generator.args.reporting_measures'

        error_msgs = ''
        warning_msgs = ''
        for measure_name, cfg_key in measure_names.items():
            measure_path = os.path.join(measures_dir, measure_name)

            if cfg_path_exists(cfg_key) or cfg_key == 'workflow_generator.args.residential_simulation_controls':
                # if they exist in the cfg, make sure they exist in the buildstock checkout
                if not os.path.exists(measure_path):
                    error_msgs += f"* {measure_name} does not exist in {buildstock_dir}. \n"

            # check the rest only if that measure exists in cfg
            if not cfg_path_exists(cfg_key):
                continue

            # check argument value types for simulation output report and timeseries csv export measures
            if measure_name in ['SimulationOutputReport', 'TimeseriesCSVExport']:
                root = get_measure_xml(os.path.join(measure_path, 'measure.xml'))
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

                for actual_argument_key in get_cfg_path(measure_names[measure_name]).keys():
                    if actual_argument_key not in expected_arguments.keys():
                        error_msgs += f"* Found unexpected argument key {actual_argument_key} for "\
                                      f"{measure_names[measure_name]} in yaml file. The available keys are: " \
                                      f"{list(expected_arguments.keys())}\n"
                        continue

                    required_args_no_default.pop(actual_argument_key, None)
                    required_args_with_default.pop(actual_argument_key, None)

                    actual_argument_value = get_cfg_path(measure_names[measure_name])[actual_argument_key]
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

        if warning_msgs:
            logger.warning(warning_msgs)

        if not error_msgs:
            return True
        else:
            logger.error(error_msgs)
            raise ValidationError(error_msgs)

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug('Generating OSW, sim_id={}'.format(sim_id))

        workflow_args = {
            'measures': []
        }
        workflow_args.update(self.cfg['workflow_generator'].get('args', {}))

        osw = {
            'id': sim_id,
            'steps': [
                {
                    "measure_dir_name": "BuildExistingModel",
                    "arguments": {
                        "number_of_buildings_represented": 1,
                        "building_id": int(building_id)
                    },
                    "measure_type": "ModelMeasure"
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
            'weather_file': 'weather/empty.epw'
        }

        # Baseline measures (not typically used in ComStock)
        osw['steps'].extend(workflow_args['measures'])

        # Upgrades
        if upgrade_idx is not None:
            measure_d = self.cfg['upgrades'][upgrade_idx]
            apply_upgrade_measure = {
                'measure_dir_name': 'ApplyUpgrade',
                'arguments': {
                    'run_measure': 1
                }
            }
            if 'upgrade_name' in measure_d:
                apply_upgrade_measure['arguments']['upgrade_name'] = measure_d['upgrade_name']
            for opt_num, option in enumerate(measure_d['options'], 1):
                apply_upgrade_measure['arguments']['option_{}'.format(opt_num)] = option['option']
                if 'lifetime' in option:
                    apply_upgrade_measure['arguments']['option_{}_lifetime'.format(opt_num)] = option['lifetime']
                if 'apply_logic' in option:
                    apply_upgrade_measure['arguments']['option_{}_apply_logic'.format(opt_num)] = \
                        self.make_apply_logic_arg(option['apply_logic'])
                for cost_num, cost in enumerate(option.get('costs', []), 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                apply_upgrade_measure['arguments']['package_apply_logic'] = \
                    self.make_apply_logic_arg(measure_d['package_apply_logic'])

            build_existing_model_idx = \
                list(map(lambda x: x['measure_dir_name'] == 'BuildExistingModel', osw['steps'])).index(True)
            osw['steps'].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        # Always-added reporting measures
        osw['steps'].extend([
            {
                "measure_dir_name": "SimulationOutputReport",
                "arguments": {},
                "measure_type": "ReportingMeasure"
            }
        ])

        if 'timeseries_csv_export' in workflow_args:
            timeseries_csv_export_args = {
                'reporting_frequency': 'Timestep',
                'inc_output_variables': False
            }
            timeseries_csv_export_args.update(workflow_args['timeseries_csv_export'])
            timeseries_measure = [{
                'measure_dir_name': 'TimeseriesCSVExport',
                'arguments': timeseries_csv_export_args,
                "measure_type": "ReportingMeasure"
            }]
            osw['steps'].extend(timeseries_measure)

        # User-specified reporting measures
        if 'reporting_measures' in workflow_args:
            for reporting_measure in workflow_args['reporting_measures']:
                if 'arguments' not in reporting_measure:
                    reporting_measure['arguments'] = {}
                reporting_measure['measure_type'] = 'ReportingMeasure'
                osw['steps'].append(reporting_measure)

        return osw
