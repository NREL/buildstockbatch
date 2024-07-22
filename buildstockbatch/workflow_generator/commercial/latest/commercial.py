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

from ...base import WorkflowGeneratorBase

logger = logging.getLogger(__name__)


def get_measure_xml(xml_path):
    tree = ElementTree.parse(xml_path)
    root = tree.getroot()
    return root


class CommercialDefaultWorkflowGenerator(WorkflowGeneratorBase):

    def validate(self):
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
        workflow_generator_args = self.cfg["workflow_generator"]["args"]
        schema_yml = re.sub(r"^ {8}", "", schema_yml, flags=re.MULTILINE)
        schema = yamale.make_schema(content=schema_yml, parser="ruamel")
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser="ruamel")
        yamale.validate(schema, data, strict=True)
        return True

    def reporting_measures(self):
        """Return a list of reporting measures to include in outputs"""
        workflow_args = self.cfg["workflow_generator"].get("args", {})

        # reporting_measures needs to return the ClassName in measure.rb, but
        # measure_dir_name in ComStock doesn't always match the ClassName
        buildstock_dir = self.cfg["buildstock_directory"]
        measures_dir = os.path.join(buildstock_dir, "measures")
        measure_class_names = []
        for m in workflow_args.get("reporting_measures", []):
            measure_dir_name = m["measure_dir_name"]
            measure_path = os.path.join(measures_dir, measure_dir_name)
            root = get_measure_xml(os.path.join(measure_path, "measure.xml"))
            measure_class_name = root.find("./class_name").text
            # Don't include OpenStudioResults, it has too many registerValues for ComStock
            if measure_class_name == "OpenStudioResults":
                continue
            measure_class_names.append(measure_class_name)

        return measure_class_names

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        logger.debug("Generating OSW, sim_id={}".format(sim_id))

        workflow_args = {"measures": []}
        workflow_args.update(self.cfg["workflow_generator"].get("args", {}))

        osw = {
            "id": sim_id,
            "steps": [
                {
                    "measure_dir_name": "BuildExistingModel",
                    "arguments": {
                        "number_of_buildings_represented": 1,
                        "building_id": int(building_id),
                    },
                    "measure_type": "ModelMeasure",
                }
            ],
            "created_at": dt.datetime.now().isoformat(),
            "measure_paths": ["measures"],
            "weather_file": "weather/empty.epw",
        }

        # Baseline measures (not typically used in ComStock)
        osw["steps"].extend(workflow_args["measures"])

        # Upgrades
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

            build_existing_model_idx = list(
                map(
                    lambda x: x["measure_dir_name"] == "BuildExistingModel",
                    osw["steps"],
                )
            ).index(True)
            osw["steps"].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        if "timeseries_csv_export" in workflow_args:
            timeseries_csv_export_args = {
                "reporting_frequency": "Timestep",
                "inc_output_variables": False,
            }
            timeseries_csv_export_args.update(workflow_args["timeseries_csv_export"])
            timeseries_measure = [
                {
                    "measure_dir_name": "TimeseriesCSVExport",
                    "arguments": timeseries_csv_export_args,
                    "measure_type": "ReportingMeasure",
                }
            ]
            osw["steps"].extend(timeseries_measure)

        # User-specified reporting measures
        if "reporting_measures" in workflow_args:
            for reporting_measure in workflow_args["reporting_measures"]:
                if "arguments" not in reporting_measure:
                    reporting_measure["arguments"] = {}
                reporting_measure["measure_type"] = "ReportingMeasure"
                osw["steps"].append(reporting_measure)

        return osw
