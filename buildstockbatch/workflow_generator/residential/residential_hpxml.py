# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential_hpxml
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Joe Robertson, Rajendra Adhikari
:copyright: (c) 2021 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from collections import Counter
import datetime as dt
import json
import logging
import os
from xml.etree import ElementTree
import yamale
from typing import Dict, Any

from ..base import WorkflowGeneratorBase
from buildstockbatch.exc import ValidationError
from buildstockbatch.workflow_generator.residential.residential_hpxml_defaults import DEFAULT_MEASURE_ARGS
from buildstockbatch.workflow_generator.residential.residential_hpxml_arg_mapping import ARG_MAP
import copy

logger = logging.getLogger(__name__)


class ResidentialHpxmlWorkflowGenerator(WorkflowGeneratorBase):

    def __init__(self, cfg, n_datapoints):
        super().__init__(cfg, n_datapoints)
        self.buildstock_dir = cfg["buildstock_directory"]
        self.measures_dir = os.path.join(self.buildstock_dir, "measures")
        self.workflow_args = self.cfg["workflow_generator"].get("args", {})
        self.default_args = copy.deepcopy(DEFAULT_MEASURE_ARGS)
        self.arg_map = copy.deepcopy(ARG_MAP)

    def validate(self):
        """Validate arguments

        :param cfg: project configuration
        :type cfg: dict
        """
        workflow_generator_args = self.cfg["workflow_generator"]["args"]
        schema_yml = os.path.join(os.path.dirname(__file__), "residential_hpxml_schema.yml")
        schema = yamale.make_schema(schema_yml, parser="ruamel")
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser="ruamel")
        yamale.validate(schema, data, strict=True)
        return self.validate_measures_and_arguments()

    def reporting_measures(self):
        """Return a list of reporting measures to include in outputs"""
        return [x["measure_dir_name"] for x in self.workflow_args.get("reporting_measures", [])]

    def _get_apply_upgrade_multipliers(self):
        measure_path = os.path.join(self.measures_dir, "ApplyUpgrade")
        root = ElementTree.parse(os.path.join(measure_path, "measure.xml")).getroot()
        multipliers = set()
        for argument in root.findall("./arguments/argument"):
            name = argument.find("./name")
            if name.text.endswith("_multiplier"):
                for choice in argument.findall("./choices/choice"):
                    value = choice.find("./value")
                    value = value.text if value is not None else ""
                    multipliers.add(value)
        return multipliers

    def _get_invalid_multipliers(self, upgrades, valid_multipliers):
        invalid_multipliers = Counter()
        for upgrade in upgrades:
            for option in upgrade["options"]:
                for cost_entry in option.get("costs", []):
                    if cost_entry["multiplier"] not in valid_multipliers:
                        invalid_multipliers[cost_entry["multiplier"]] += 1
        return invalid_multipliers

    def validate_measures_and_arguments(self):
        error_msgs = ""
        warning_msgs = ""
        if "upgrades" in self.cfg:
            # For ApplyUpgrade measure, verify that all the cost_multipliers used are correct
            valid_multipliers = self._get_apply_upgrade_multipliers()
            invalid_multipliers = self._get_invalid_multipliers(self.cfg["upgrades"], valid_multipliers)
            if invalid_multipliers:
                error_msgs += "* The following multipliers values are invalid: \n"
                for multiplier, count in invalid_multipliers.items():
                    error_msgs += f"    '{multiplier}' - Used {count} times \n"
                error_msgs += f"    The list of valid multipliers are {valid_multipliers}.\n"

        for reporting_measure in self.workflow_args.get("reporting_measures", []):
            if not os.path.isdir(os.path.join(self.measures_dir, reporting_measure["measure_dir_name"])):
                error_msgs += f"* Reporting measure '{reporting_measure['measure_dir_name']}' not found\n"

        if warning_msgs:
            logger.warning(warning_msgs)

        if not error_msgs:
            return True

        logger.error(error_msgs)
        raise ValidationError(error_msgs)

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug("Generating OSW, sim_id={}".format(sim_id))

        workflow_args = copy.deepcopy(self.workflow_args)

        measure_args = self._get_mapped_args(workflow_args)  # start with the mapped arguments

        measure_args["BuildExistingModel"].update(
            {
                "building_id": building_id,
                "sample_weight": self.cfg["baseline"]["n_buildings_represented"] / self.n_datapoints,
            }
        )
        debug = workflow_args.get("debug", False)

        measure_args_mapping = {
            "build_existing_model": "BuildExistingModel",
            "hpxml_to_openstudio": "HPXMLtoOpenStudio",
            "simulation_output_report": "ReportSimulationOutput",
            "report_hpxml_output": "ReportHPXMLOutput",
            "report_utility_bills": "ReportUtilityBills",
            "upgrade_costs": "UpgradeCosts",
            "server_directory_cleanup": "ServerDirectoryCleanup",
        }

        steps = []
        for key, measure_dir_name in measure_args_mapping.items():
            if measure_dir_name not in measure_args:
                measure_args[measure_dir_name] = {}

            measure_args[measure_dir_name].update(
                self._get_measure_args(workflow_args.get(key, {}), measure_dir_name, debug)
            )
            steps.append(
                {
                    "measure_dir_name": measure_dir_name,
                    "arguments": measure_args[measure_dir_name],
                }
            )

        osw = {
            "id": sim_id,
            "steps": steps,
            "created_at": dt.datetime.now().isoformat(),
            "measure_paths": ["measures", "resources/hpxml-measures"],
            "run_options": {"skip_zip_results": True},
        }
        for measure in reversed(workflow_args.get("measures", [])):
            osw["steps"].insert(2, measure)

        self.add_upgrade_step_to_osw(upgrade_idx, osw)

        for reporting_measure in self.workflow_args.get("reporting_measures", []):
            if "arguments" not in reporting_measure:
                reporting_measure["arguments"] = {}
            reporting_measure["measure_type"] = "ReportingMeasure"
            osw["steps"].insert(-1, reporting_measure)  # right before ServerDirectoryCleanup

        return osw

    def add_upgrade_step_to_osw(self, upgrade_idx, osw):
        num_regular_upgrades = len(self.cfg.get("upgrades", []))
        if upgrade_idx is None:
            return

        if upgrade_idx < num_regular_upgrades:
            measure_d = self.cfg["upgrades"][upgrade_idx]
            apply_upgrade_measure = {
                "measure_dir_name": "ApplyUpgrade",
                "arguments": {"run_measure": 1},
            }
            if "upgrade_name" in measure_d:
                apply_upgrade_measure["arguments"]["upgrade_name"] = measure_d["upgrade_name"]
            for opt_num, option in enumerate(measure_d["options"], 1):
                apply_upgrade_measure["arguments"]["option_{}".format(opt_num)] = option["option"]
                if "lifetime" in option:
                    apply_upgrade_measure["arguments"]["option_{}_lifetime".format(opt_num)] = option["lifetime"]
                if "apply_logic" in option:
                    apply_upgrade_measure["arguments"]["option_{}_apply_logic".format(opt_num)] = (
                        self.make_apply_logic_arg(option["apply_logic"])
                    )
                for cost_num, cost in enumerate(option.get("costs", []), 1):
                    for arg in ("value", "multiplier"):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure["arguments"]["option_{}_cost_{}_{}".format(opt_num, cost_num, arg)] = (
                            cost[arg]
                        )
            if "package_apply_logic" in measure_d:
                apply_upgrade_measure["arguments"]["package_apply_logic"] = self.make_apply_logic_arg(
                    measure_d["package_apply_logic"]
                )
            osw["steps"].insert(1, apply_upgrade_measure)  # right after BuildExistingModel

    def _get_measure_args(self, workflow_block_args, measure_dir_name, debug):
        """
        Get the arguments to the measure from the workflow_args and defaults. The arguments are filtered based
        on the measure's measure.xml file. If an argument is not found in the measure.xml file, it is not
        passed to the measure and a warning is logged.
        """
        xml_args = self.get_measure_arguments_from_xml(self.buildstock_dir, measure_dir_name)
        measure_args = self.default_args.get(measure_dir_name, {}).copy()
        measure_args.update(workflow_block_args)
        for key in list(measure_args.keys()):
            if key not in xml_args:
                location = "workflow_generator" if key in workflow_block_args else "defaults"
                logger.warning(
                    f"'{key}' in {location} not found in '{measure_dir_name}'. This key will not be passed"
                    " to the measure. This warning is expected if you are using older version of ResStock."
                )
                del measure_args[key]
        if "debug" in xml_args:
            measure_args["debug"] = debug
        return measure_args

    def _get_mapped_args(self, workflow_args):
        """
        Get the arguments to various measures from the workflow_args. The mapping is defined in the ARG_MAP
        """
        measure_args = {}
        for yaml_blockname, arg_maps in self.arg_map.items():
            if yaml_blockname not in workflow_args:
                continue
            yaml_block = workflow_args[yaml_blockname]
            self.recursive_dict_update(measure_args, self._get_mapped_args_from_block(yaml_block, arg_maps))
        return measure_args

    @staticmethod
    def get_measure_arguments_from_xml(buildstock_dir, measure_dir_name: str):
        for measure_path in ["measures", "resources/hpxml-measures"]:
            measure_dir_path = os.path.join(buildstock_dir, measure_path, measure_dir_name)
            if os.path.isdir(measure_dir_path):
                break
        else:
            raise ValueError(f"Measure '{measure_dir_name}' not found in any of the measure directories")
        measure_xml_path = os.path.join(measure_dir_path, "measure.xml")
        if not os.path.isfile(measure_xml_path):
            raise ValueError(f"Measure '{measure_dir_name}' does not have a measure xml file")
        arguments = set()
        root = ElementTree.parse(measure_xml_path).getroot()
        for argument in root.findall("./arguments/argument"):
            name = argument.find("./name").text
            arguments.add(name)
        return arguments

    @staticmethod
    def recursive_dict_update(base_dict, new_dict):
        """
        Fully update a dictionary with another dictionary, traversing nested dictionaries
        """
        for key, value in new_dict.items():
            if isinstance(value, dict):
                base_dict.setdefault(key, {})
                ResidentialHpxmlWorkflowGenerator.recursive_dict_update(base_dict[key], value)
            else:
                base_dict[key] = value
        return True

    @staticmethod
    def _get_condensed_block(yaml_block):
        """
        If the yaml_block is a list of dicts, condense it into a single dict
        with values being the list of values from the dicts in the list. If
        a key is missing in a particular block, use empty string as the value.

        The purpose of this function is to convert the certain blocks like utility_bills
        and emissions into a single block with list values to be passed to the measures.
        Example Input:
        [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4}
        ]
        Example Output:
        {
            "a": [1, 3],
            "b": [2, 4]
        }

        Example Input2:
        [
            {"a": 1, "b": 2},
            {"a": 3}
        ]
        Example Output2:
        {
            "a": [1, 3],
            "b": [2, ""]
        }
        """
        if not isinstance(yaml_block, list):
            return yaml_block
        condensed_block = {}
        all_keys = set()
        for block in yaml_block:
            all_keys.update(block.keys())
        for key in all_keys:
            condensed_block[key] = [block.get(key, "") for block in yaml_block]
        return condensed_block

    @staticmethod
    def _get_mapped_args_from_block(block, arg_maps: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
        """
        Get the arguments to meaures using the ARG_MAP for the given block.
        The block is either a dict or a list of dicts. If it is a list of dicts, it is
        first condensed into a single dict using _get_condensed_block function.

        The arg_maps is a dictionary with the destination measure name as the key
        and a dictionary as the value. The value dictionary has the source argument name as the key
        and the destination argument name as the value. The source argument name is the key in the
        yaml and destination argument name is the key to be passed to the measure.

        If a value is a list, it is joined into a comma separated string.
        If a value is a list of dicts, then the "name" key is used to join into a comma separated string.
        Otherwise, the value is passed as is.
        Example Input:
        {
            "utility_bills": [
                {"scenario_name": "scenario1", "simple_filepath": "file1"},
                {"scenario_name": "scenario2", "simple_filepath": "file2"}
            ],
            ...
            "report_simulation_output": {
                "output_variables": [
                    {"name": "var1"},
                    {"name": "var2"}
                ]
            }
        }
        Example output:
        {
            "BuildExistingModel": {
                "utility_bill_scenario_names": "scenario1,scenario2",
                ...
            },
            "ReportSimulationOutput": {
                "user_output_variables": "var1,va2",
        }
        """
        block_count = len(block) if isinstance(block, list) else 1
        block = ResidentialHpxmlWorkflowGenerator._get_condensed_block(block)
        measure_args = {}
        for measure_dir_name, arg_map in arg_maps.items():
            mapped_args = measure_args.setdefault(measure_dir_name, {})
            for source_arg, dest_arg in arg_map.items():
                if source_arg in block:
                    # Use pop to remove the key from the block since it is already consumed
                    if isinstance(block[source_arg], list):
                        if isinstance(block[source_arg][0], dict):
                            mapped_args[dest_arg] = ",".join(str(v.get("name", "")) for v in block.pop(source_arg))
                        else:
                            mapped_args[dest_arg] = ",".join(str(v) for v in block.pop(source_arg))
                    else:
                        mapped_args[dest_arg] = block.pop(source_arg)
                else:
                    mapped_args[dest_arg] = ",".join([""] * block_count)

        return measure_args
