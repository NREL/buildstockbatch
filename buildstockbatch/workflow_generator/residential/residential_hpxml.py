# -*- coding: utf-8 -*-

"""
buildstockbatch.workflow_generator.residential_hpxml
~~~~~~~~~~~~~~~
This object contains the residential classes for generating OSW files from individual samples

:author: Joe Robertson
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


from ..base import WorkflowGeneratorBase
from buildstockbatch.exc import ValidationError
from buildstockbatch.workflow_generator.residential.residential_hpxml_defaults import DEFAULT_MEASURE_ARGS
from buildstockbatch.workflow_generator.residential.residential_hpxml_arg_mapping import BUILD_EXISTING_MODEL_ARG_MAP

logger = logging.getLogger(__name__)


def get_measure_xml(xml_path):
    tree = ElementTree.parse(xml_path)
    root = tree.getroot()
    return root


def get_measure_arguments(xml_path):
    arguments = set()
    if os.path.isfile(xml_path):
        root = get_measure_xml(xml_path)
        for argument in root.findall("./arguments/argument"):
            name = argument.find("./name").text
            arguments.add(name)
    return arguments


class ResidentialHpxmlWorkflowGenerator(WorkflowGeneratorBase):

    def __init__(self, cfg, n_datapoints):
        super().__init__(cfg, n_datapoints)
        self.buildstock_dir = cfg["buildstock_directory"]
        self.measures_dir = os.path.join(self.buildstock_dir, "measures")

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
        workflow_args = self.cfg["workflow_generator"].get("args", {})
        return [x["measure_dir_name"] for x in workflow_args.get("reporting_measures", [])]

    def _get_apply_upgrade_multipliers(self):
        measure_path = os.path.join(self.measures_dir, "ApplyUpgrade")
        root = get_measure_xml(os.path.join(measure_path, "measure.xml"))
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

        workflow_args = self.cfg["workflow_generator"].get("args", {})

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

        for reporting_measure in workflow_args.get("reporting_measures", []):
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
        workflow_args = self.cfg["workflow_generator"].get("args", {})

        bld_exist_model_args = {
            "building_id": building_id,
            "sample_weight": self.cfg["baseline"]["n_buildings_represented"] / self.n_datapoints,
        }

        bld_exist_model_args.update(DEFAULT_MEASURE_ARGS["measures/BuildExistingModel"])
        bld_exist_model_args.update(workflow_args.get("build_existing_model", {}))

        # add_component_loads argument needs to be passed to HPXMLtoOpenStudio measure
        add_component_loads = bld_exist_model_args.pop("add_component_loads")

        bld_exist_model_args.update(self._get_emission_args(workflow_args))
        bld_exist_model_args.update(self._get_bill_args(workflow_args))
        sim_out_rep_args = self._get_sim_out_args(workflow_args)
        util_bills_rep_args = self._get_bill_report_args()
        server_dir_cleanup_args = self._get_server_dir_cleanup_args(workflow_args)
        debug = workflow_args.get("debug", False)
        osw = {
            "id": sim_id,
            "steps": [
                {
                    "measure_dir_name": "BuildExistingModel",
                    "arguments": bld_exist_model_args,
                }
            ],
            "created_at": dt.datetime.now().isoformat(),
            "measure_paths": ["measures", "resources/hpxml-measures"],
            "run_options": {"skip_zip_results": True},
        }

        osw["steps"].extend(
            [
                {
                    "measure_dir_name": "HPXMLtoOpenStudio",
                    "arguments": {
                        "hpxml_path": "../../run/home.xml",
                        "output_dir": "../../run",
                        "debug": debug,
                        "add_component_loads": add_component_loads,
                        "skip_validation": True,
                    },
                }
            ]
        )

        osw["steps"].extend(workflow_args.get("measures", []))

        osw["steps"].extend(
            [
                {
                    "measure_dir_name": "ReportSimulationOutput",
                    "arguments": sim_out_rep_args,
                },
                {"measure_dir_name": "ReportHPXMLOutput", "arguments": {}},
                {
                    "measure_dir_name": "ReportUtilityBills",
                    "arguments": util_bills_rep_args,
                },
                {"measure_dir_name": "UpgradeCosts", "arguments": {"debug": debug}},
                {
                    "measure_dir_name": "ServerDirectoryCleanup",
                    "arguments": server_dir_cleanup_args,
                },
            ]
        )

        if upgrade_idx is not None:
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

            build_existing_model_idx = [x["measure_dir_name"] == "BuildExistingModel" for x in osw["steps"]].index(True)
            osw["steps"].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        for reporting_measure in workflow_args.get("reporting_measures", []):
            if "arguments" not in reporting_measure:
                reporting_measure["arguments"] = {}
            reporting_measure["measure_type"] = "ReportingMeasure"
            osw["steps"].insert(-1, reporting_measure)  # right before ServerDirectoryCleanup

        return osw

    def _get_server_dir_cleanup_args(self, workflow_args):
        server_dir_cleanup_args = DEFAULT_MEASURE_ARGS["measures/ServerDirectoryCleanup"].copy()
        server_dir_cleanup_args.update(workflow_args.get("server_directory_cleanup", {}))
        return server_dir_cleanup_args

    def _get_bill_report_args(self):
        util_bills_rep_args = DEFAULT_MEASURE_ARGS["resources/hpxml-measures/ReportUtilityBills"].copy()
        return util_bills_rep_args

    def _get_sim_out_args(self, workflow_args):
        """
        Get the simulation output related arguments from the workflow_args to be added to the ReportSimulationOutput
        measure argument
        """
        sim_out_rep_args = DEFAULT_MEASURE_ARGS["resources/hpxml-measures/ReportSimulationOutput"].copy()
        sim_out_rep_args.update(workflow_args["simulation_output_report"])
        if "output_variables" in sim_out_rep_args:
            # convert list to comma separated string and change key name to user_output_variables
            sim_out_rep_args["user_output_variables"] = ",".join(
                [str(s.get("name")) for s in sim_out_rep_args["output_variables"]]
            )
            sim_out_rep_args.pop("output_variables")

        return sim_out_rep_args

    def _get_mapped_args(self, workflow2measure, entries):
        """
        Get the arguments from the workflow_args to be added to the BuildExistingModel measure argument
        by mapping the workflow argument names to the measure argument names and aggregating the values
        across multiple entries as a comma separated string
        """
        mapped_args = {}
        for workflow_arg_name, measure_arg_name in workflow2measure.items():
            workflow_arg_vals = [str(entry.get(workflow_arg_name, "")) for entry in entries]
            mapped_args[measure_arg_name] = ",".join(workflow_arg_vals)
        return mapped_args

    def _get_bill_args(self, workflow_args):
        """
        Get the utility bill related arguments from the workflow_args to be added to the BuildExistingModel
        measure argument
        """
        bill_args = {}
        if "utility_bills" not in workflow_args:
            return bill_args

        utility_bills = workflow_args["utility_bills"]
        arg_name_map = BUILD_EXISTING_MODEL_ARG_MAP["utility_bills"]
        return self._get_mapped_args(arg_name_map, utility_bills)

    def _get_emission_args(self, workflow_args):
        """
        Get the emission related arguments from the workflow_args to be added to the BuildExistingModel
        measure argument
        """
        emission_args = {}
        if "emissions" not in workflow_args:
            return emission_args
        arg_name_mapping = BUILD_EXISTING_MODEL_ARG_MAP["emissions"]
        return self._get_mapped_args(arg_name_mapping, workflow_args["emissions"])
