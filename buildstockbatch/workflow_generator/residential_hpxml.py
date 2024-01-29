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


def get_measure_arguments(xml_path):
    arguments = []
    if os.path.isfile(xml_path):
        root = get_measure_xml(xml_path)
        for argument in root.findall("./arguments/argument"):
            name = argument.find("./name").text
            arguments.append(name)
    return arguments


class ResidentialHpxmlWorkflowGenerator(WorkflowGeneratorBase):
    @classmethod
    def validate(cls, cfg):
        """Validate arguments

        :param cfg: project configuration
        :type cfg: dict
        """
        schema_yml = """
        build_existing_model: include('build-existing-model-spec', required=False)
        emissions: list(include('emission-scenario-spec'), required=False)
        utility_bills: list(include('utility-bill-scenario-spec'), required=False)
        measures: list(include('measure-spec'), required=False)
        reporting_measures: list(include('measure-spec'), required=False)
        simulation_output_report: include('sim-output-report-spec', required=False)
        server_directory_cleanup: include('server-dir-cleanup-spec', required=False)
        debug: bool(required=False)
        ---
        build-existing-model-spec:
            simulation_control_timestep: enum(60, 30, 20, 15, 12, 10, 6, 5, 4, 3, 2, 1, required=False)
            simulation_control_run_period_begin_month: int(required=False)
            simulation_control_run_period_begin_day_of_month: int(required=False)
            simulation_control_run_period_end_month: int(required=False)
            simulation_control_run_period_end_day_of_month: int(required=False)
            simulation_control_run_period_calendar_year: int(required=False)
            add_component_loads: bool(required=False)
        emission-scenario-spec:
            scenario_name: str(required=True)
            type: str(required=True)
            elec_folder: str(required=True)
            gas_value: num(required=False)
            propane_value: num(required=False)
            oil_value: num(required=False)
            wood_value: num(required=False)
        utility-bill-scenario-spec:
            scenario_name: str(required=True)
            simple_filepath: str(required=False)
            detailed_filepath: str(required=False)
            elec_fixed_charge: num(required=False)
            elec_marginal_rate: num(required=False)
            gas_fixed_charge: num(required=False)
            gas_marginal_rate: num(required=False)
            propane_fixed_charge: num(required=False)
            propane_marginal_rate: num(required=False)
            oil_fixed_charge: num(required=False)
            oil_marginal_rate: num(required=False)
            wood_fixed_charge: num(required=False)
            wood_marginal_rate: num(required=False)
            pv_compensation_type: enum('NetMetering', 'FeedInTariff', required=False)
            pv_net_metering_annual_excess_sellback_rate_type: enum('User-Specified', 'Retail Electricity Cost', required=False)
            pv_net_metering_annual_excess_sellback_rate: num(required=False)
            pv_feed_in_tariff_rate: num(required=False)
            pv_monthly_grid_connection_fee_units: enum('$', '$/kW', required=False)
            pv_monthly_grid_connection_fee: num(required=False)
        sim-output-report-spec:
            timeseries_frequency: enum('none', 'timestep', 'hourly', 'daily', 'monthly', required=False)
            include_timeseries_total_consumptions: bool(required=False)
            include_timeseries_fuel_consumptions: bool(required=False)
            include_timeseries_end_use_consumptions: bool(required=False)
            include_timeseries_emissions: bool(required=False)
            include_timeseries_emission_fuels: bool(required=False)
            include_timeseries_emission_end_uses: bool(required=False)
            include_timeseries_hot_water_uses: bool(required=False)
            include_timeseries_total_loads: bool(required=False)
            include_timeseries_component_loads: bool(required=False)
            include_timeseries_unmet_hours: bool(required=False)
            include_timeseries_zone_temperatures: bool(required=False)
            include_timeseries_airflows: bool(required=False)
            include_timeseries_weather: bool(required=False)
            include_timeseries_resilience: bool(required=False)
            timeseries_timestamp_convention: enum('start', 'end', required=False)
            timeseries_num_decimal_places: int(required=False)
            add_timeseries_dst_column: bool(required=False)
            add_timeseries_utc_column: bool(required=False)
            output_variables: list(include('output-var-spec'), required=False)
        output-var-spec:
            name: str(required=True)
        measure-spec:
            measure_dir_name: str(required=True)
            arguments: map(required=False)
        server-dir-cleanup-spec:
            retain_in_osm: bool(required=False)
            retain_in_idf: bool(required=False)
            retain_pre_process_idf: bool(required=False)
            retain_eplusout_audit: bool(required=False)
            retain_eplusout_bnd: bool(required=False)
            retain_eplusout_eio: bool(required=False)
            retain_eplusout_end: bool(required=False)
            retain_eplusout_err: bool(required=False)
            retain_eplusout_eso: bool(required=False)
            retain_eplusout_mdd: bool(required=False)
            retain_eplusout_mtd: bool(required=False)
            retain_eplusout_rdd: bool(required=False)
            retain_eplusout_shd: bool(required=False)
            retain_eplusout_msgpack: bool(required=False)
            retain_eplustbl_htm: bool(required=False)
            retain_stdout_energyplus: bool(required=False)
            retain_stdout_expandobject: bool(required=False)
            retain_schedules_csv: bool(required=False)
            debug: bool(required=False)
        """  # noqa E501
        workflow_generator_args = cfg["workflow_generator"]["args"]
        schema_yml = re.sub(r"^ {8}", "", schema_yml, flags=re.MULTILINE)
        schema = yamale.make_schema(content=schema_yml, parser="ruamel")
        data = yamale.make_data(content=json.dumps(workflow_generator_args), parser="ruamel")
        yamale.validate(schema, data, strict=True)
        return cls.validate_measures_and_arguments(cfg)

    def reporting_measures(self):
        """Return a list of reporting measures to include in outputs"""
        workflow_args = self.cfg["workflow_generator"].get("args", {})
        return [x["measure_dir_name"] for x in workflow_args.get("reporting_measures", [])]

    @staticmethod
    def validate_measures_and_arguments(cfg):
        buildstock_dir = cfg["buildstock_directory"]
        measures_dir = os.path.join(buildstock_dir, "measures")

        measure_names = {
            "BuildExistingModel": "baseline",
            "ApplyUpgrade": "upgrades",
        }

        def cfg_path_exists(cfg_path):
            if cfg_path is None:
                return False
            path_items = cfg_path.split(".")
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
            path_items = cfg_path.split(".")
            a = cfg
            for path_item in path_items:
                try:
                    a = a[path_item]
                except KeyError:
                    return None
            return a

        workflow_args = cfg["workflow_generator"].get("args", {})
        if "reporting_measures" in workflow_args.keys():
            for reporting_measure in workflow_args["reporting_measures"]:
                measure_names[reporting_measure["measure_dir_name"]] = "workflow_generator.args.reporting_measures"

        error_msgs = ""
        warning_msgs = ""
        for measure_name, cfg_key in measure_names.items():
            measure_path = os.path.join(measures_dir, measure_name)

            # check the rest only if that measure exists in cfg
            if not cfg_path_exists(cfg_key):
                continue

            if measure_name in ["ApplyUpgrade"]:
                # For ApplyUpgrade measure, verify that all the cost_multipliers used are correct
                root = get_measure_xml(os.path.join(measure_path, "measure.xml"))
                valid_multipliers = set()
                for argument in root.findall("./arguments/argument"):
                    name = argument.find("./name")
                    if name.text.endswith("_multiplier"):
                        for choice in argument.findall("./choices/choice"):
                            value = choice.find("./value")
                            value = value.text if value is not None else ""
                            valid_multipliers.add(value)
                invalid_multipliers = Counter()
                for upgrade_count, upgrade in enumerate(cfg["upgrades"]):
                    for option_count, option in enumerate(upgrade["options"]):
                        for cost_indx, cost_entry in enumerate(option.get("costs", [])):
                            if cost_entry["multiplier"] not in valid_multipliers:
                                invalid_multipliers[cost_entry["multiplier"]] += 1

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

    def create_osw(self, sim_id, building_id, upgrade_idx):
        """
        Generate and return the osw as a python dict

        :param sim_id: simulation id, looks like 'bldg0000001up01'
        :param building_id: integer building id to use from the sampled buildstock.csv
        :param upgrade_idx: integer index of the upgrade scenario to apply, None if baseline
        """
        # Default argument values
        workflow_args = {
            "build_existing_model": {},
            "measures": [],
            "simulation_output_report": {},
            "server_directory_cleanup": {},
        }
        workflow_args.update(self.cfg["workflow_generator"].get("args", {}))

        logger.debug("Generating OSW, sim_id={}".format(sim_id))

        sim_ctl_args = {
            "simulation_control_timestep": 60,
            "simulation_control_run_period_begin_month": 1,
            "simulation_control_run_period_begin_day_of_month": 1,
            "simulation_control_run_period_end_month": 12,
            "simulation_control_run_period_end_day_of_month": 31,
            "simulation_control_run_period_calendar_year": 2007,
            "add_component_loads": False,
        }

        bld_exist_model_args = {
            "building_id": building_id,
            "sample_weight": self.cfg["baseline"]["n_buildings_represented"] / self.n_datapoints,
        }

        bld_exist_model_args.update(sim_ctl_args)
        bld_exist_model_args.update(workflow_args["build_existing_model"])

        add_component_loads = False
        if "add_component_loads" in bld_exist_model_args:
            add_component_loads = bld_exist_model_args["add_component_loads"]
            bld_exist_model_args.pop("add_component_loads")

        if "emissions" in workflow_args:
            emissions = workflow_args["emissions"]
            emissions_map = [
                ["emissions_scenario_names", "scenario_name"],
                ["emissions_types", "type"],
                ["emissions_electricity_folders", "elec_folder"],
                ["emissions_natural_gas_values", "gas_value"],
                ["emissions_propane_values", "propane_value"],
                ["emissions_fuel_oil_values", "oil_value"],
                ["emissions_wood_values", "wood_value"],
            ]
            for arg, item in emissions_map:
                bld_exist_model_args[arg] = ",".join([str(s.get(item, "")) for s in emissions])

        buildstock_dir = self.cfg["buildstock_directory"]
        measures_dir = os.path.join(buildstock_dir, "measures")
        measure_path = os.path.join(measures_dir, "BuildExistingModel")
        bld_exist_model_args_avail = get_measure_arguments(os.path.join(measure_path, "measure.xml"))

        if "utility_bills" in workflow_args:
            utility_bills = workflow_args["utility_bills"]
            utility_bills_map = [
                ["utility_bill_scenario_names", "scenario_name"],
                ["utility_bill_simple_filepaths", "simple_filepath"],
                ["utility_bill_detailed_filepaths", "detailed_filepath"],
                ["utility_bill_electricity_fixed_charges", "elec_fixed_charge"],
                ["utility_bill_electricity_marginal_rates", "elec_marginal_rate"],
                ["utility_bill_natural_gas_fixed_charges", "gas_fixed_charge"],
                ["utility_bill_natural_gas_marginal_rates", "gas_marginal_rate"],
                ["utility_bill_propane_fixed_charges", "propane_fixed_charge"],
                ["utility_bill_propane_marginal_rates", "propane_marginal_rate"],
                ["utility_bill_fuel_oil_fixed_charges", "oil_fixed_charge"],
                ["utility_bill_fuel_oil_marginal_rates", "oil_marginal_rate"],
                ["utility_bill_wood_fixed_charges", "wood_fixed_charge"],
                ["utility_bill_wood_marginal_rates", "wood_marginal_rate"],
                ["utility_bill_pv_compensation_types", "pv_compensation_type"],
                [
                    "utility_bill_pv_net_metering_annual_excess_sellback_rate_types",
                    "pv_net_metering_annual_excess_sellback_rate_type",
                ],
                [
                    "utility_bill_pv_net_metering_annual_excess_sellback_rates",
                    "pv_net_metering_annual_excess_sellback_rate",
                ],
                ["utility_bill_pv_feed_in_tariff_rates", "pv_feed_in_tariff_rate"],
                [
                    "utility_bill_pv_monthly_grid_connection_fee_units",
                    "pv_monthly_grid_connection_fee_units",
                ],
                [
                    "utility_bill_pv_monthly_grid_connection_fees",
                    "pv_monthly_grid_connection_fee",
                ],
            ]
            for arg, item in utility_bills_map:
                if arg in bld_exist_model_args_avail:
                    bld_exist_model_args[arg] = ",".join([str(s.get(item, "")) for s in utility_bills])

        sim_out_rep_args = {
            "timeseries_frequency": "none",
            "include_timeseries_total_consumptions": False,
            "include_timeseries_fuel_consumptions": False,
            "include_timeseries_end_use_consumptions": True,
            "include_timeseries_emissions": False,
            "include_timeseries_emission_fuels": False,
            "include_timeseries_emission_end_uses": False,
            "include_timeseries_hot_water_uses": False,
            "include_timeseries_total_loads": True,
            "include_timeseries_component_loads": False,
            "include_timeseries_zone_temperatures": False,
            "include_timeseries_airflows": False,
            "include_timeseries_weather": False,
            "timeseries_timestamp_convention": "end",
            "add_timeseries_dst_column": True,
            "add_timeseries_utc_column": True,
        }

        measures_dir = os.path.join(buildstock_dir, "resources/hpxml-measures")
        measure_path = os.path.join(measures_dir, "ReportSimulationOutput")
        sim_out_rep_args_avail = get_measure_arguments(os.path.join(measure_path, "measure.xml"))

        if "include_annual_total_consumptions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_total_consumptions"] = True

        if "include_annual_fuel_consumptions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_fuel_consumptions"] = True

        if "include_annual_end_use_consumptions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_end_use_consumptions"] = True

        if "include_annual_system_use_consumptions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_system_use_consumptions"] = False

        if "include_annual_emissions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_emissions"] = True

        if "include_annual_emission_fuels" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_emission_fuels"] = True

        if "include_annual_emission_end_uses" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_emission_end_uses"] = True

        if "include_annual_total_loads" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_total_loads"] = True

        if "include_annual_unmet_hours" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_unmet_hours"] = True

        if "include_annual_peak_fuels" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_peak_fuels"] = True

        if "include_annual_peak_loads" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_peak_loads"] = True

        if "include_annual_component_loads" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_component_loads"] = True

        if "include_annual_hot_water_uses" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_hot_water_uses"] = True

        if "include_annual_hvac_summary" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_hvac_summary"] = True

        if "include_annual_resilience" in sim_out_rep_args_avail:
            sim_out_rep_args["include_annual_resilience"] = True

        if "include_timeseries_system_use_consumptions" in sim_out_rep_args_avail:
            sim_out_rep_args["include_timeseries_system_use_consumptions"] = False

        if "include_timeseries_unmet_hours" in sim_out_rep_args_avail:
            sim_out_rep_args["include_timeseries_unmet_hours"] = False

        if "include_timeseries_resilience" in sim_out_rep_args_avail:
            sim_out_rep_args["include_timeseries_resilience"] = False

        if "timeseries_num_decimal_places" in sim_out_rep_args_avail:
            sim_out_rep_args["timeseries_num_decimal_places"] = 3

        sim_out_rep_args.update(workflow_args["simulation_output_report"])

        if "output_variables" in sim_out_rep_args:
            output_variables = sim_out_rep_args["output_variables"]
            sim_out_rep_args["user_output_variables"] = ",".join([str(s.get("name")) for s in output_variables])
            sim_out_rep_args.pop("output_variables")

        util_bills_rep_args = {}

        measures_dir = os.path.join(buildstock_dir, "resources/hpxml-measures")
        measure_path = os.path.join(measures_dir, "ReportUtilityBills")
        util_bills_rep_args_avail = get_measure_arguments(os.path.join(measure_path, "measure.xml"))

        if "include_annual_bills" in util_bills_rep_args_avail:
            util_bills_rep_args["include_annual_bills"] = True

        if "include_monthly_bills" in util_bills_rep_args_avail:
            util_bills_rep_args["include_monthly_bills"] = False

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

        debug = False
        if "debug" in workflow_args:
            debug = workflow_args["debug"]

        server_dir_cleanup_args = {
            "retain_in_osm": False,
            "retain_in_idf": True,
            "retain_pre_process_idf": False,
            "retain_eplusout_audit": False,
            "retain_eplusout_bnd": False,
            "retain_eplusout_eio": False,
            "retain_eplusout_end": False,
            "retain_eplusout_err": False,
            "retain_eplusout_eso": False,
            "retain_eplusout_mdd": False,
            "retain_eplusout_mtd": False,
            "retain_eplusout_rdd": False,
            "retain_eplusout_shd": False,
            "retain_eplusout_msgpack": False,
            "retain_eplustbl_htm": False,
            "retain_stdout_energyplus": False,
            "retain_stdout_expandobject": False,
            "retain_schedules_csv": True,
            "debug": debug,
        }
        server_dir_cleanup_args.update(workflow_args["server_directory_cleanup"])

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

        osw["steps"].extend(workflow_args["measures"])

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
                    apply_upgrade_measure["arguments"][
                        "option_{}_apply_logic".format(opt_num)
                    ] = self.make_apply_logic_arg(option["apply_logic"])
                for cost_num, cost in enumerate(option.get("costs", []), 1):
                    for arg in ("value", "multiplier"):
                        if arg not in cost:
                            continue
                        apply_upgrade_measure["arguments"][
                            "option_{}_cost_{}_{}".format(opt_num, cost_num, arg)
                        ] = cost[arg]
            if "package_apply_logic" in measure_d:
                apply_upgrade_measure["arguments"]["package_apply_logic"] = self.make_apply_logic_arg(
                    measure_d["package_apply_logic"]
                )

            build_existing_model_idx = [x["measure_dir_name"] == "BuildExistingModel" for x in osw["steps"]].index(True)
            osw["steps"].insert(build_existing_model_idx + 1, apply_upgrade_measure)

        if "reporting_measures" in workflow_args:
            for reporting_measure in workflow_args["reporting_measures"]:
                if "arguments" not in reporting_measure:
                    reporting_measure["arguments"] = {}
                reporting_measure["measure_type"] = "ReportingMeasure"
                osw["steps"].insert(-1, reporting_measure)  # right before ServerDirectoryCleanup

        return osw
