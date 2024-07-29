from buildstockbatch.workflow_generator.residential.latest.residential_hpxml import ResidentialHpxmlWorkflowGenerator
from buildstockbatch.workflow_generator.residential.latest.residential_hpxml_defaults import DEFAULT_MEASURE_ARGS
from buildstockbatch.workflow_generator.residential.latest.residential_hpxml_arg_mapping import ARG_MAP
from testfixtures import LogCapture
import os
import yamale
import logging
import copy
import itertools
import pytest
import pathlib

resstock_directory = pathlib.Path(__file__).parent / "testing_resstock_data"

test_cfg = {
    "buildstock_directory": resstock_directory,
    "baseline": {"n_buildings_represented": 100},
    "workflow_generator": {
        "type": "residential_hpxml",
        "args": {
            "debug": True,
            "build_existing_model": {
                "simulation_control_timestep": 15,
                "simulation_control_run_period_begin_month": 2,
                "simulation_control_run_period_begin_day_of_month": 1,
                "simulation_control_run_period_end_month": 2,
                "simulation_control_run_period_end_day_of_month": 28,
                "simulation_control_run_period_calendar_year": 2010,
                "add_component_loads": True,
            },
            "emissions": [
                {
                    "scenario_name": "LRMER_MidCase_15",
                    "type": "CO2e",
                    "elec_folder": "data/emissions/cambium/2022/LRMER_MidCase_15",
                    "gas_value": 147.3,
                    "propane_value": 177.8,
                    "oil_value": 195.9,
                    "wood_value": 200.0,
                },
                {
                    "scenario_name": "LRMER_HighCase_15",
                    "type": "CO2e",
                    "elec_folder": "data/emissions/cambium/2022/LRMER_HighCase_15",
                    "gas_value": 187.3,
                    "propane_value": 187.8,
                    "oil_value": 199.9,
                    "wood_value": 250.0,
                },
            ],
            "utility_bills": [
                {"scenario_name": "Bills", "elc_fixed_charge": 10.0, "elc_marginal_rate": 0.12},
                {"scenario_name": "Bills2", "gas_fixed_charge": 12.0, "gas_marginal_rate": 0.15},
            ],
            "simulation_output_report": {
                "timeseries_frequency": "hourly",
                "include_timeseries_total_consumptions": True,
                "include_timeseries_end_use_consumptions": True,
                "include_timeseries_total_loads": True,
                "include_timeseries_zone_temperatures": True,
                "output_variables": [
                    {"name": "Zone Mean Air Temperature"},
                    {"name": "Zone People Occupant Count"},
                ],
                "include_monthly_bills": True,
            },
            "server_directory_cleanup": {"retain_in_osm": True, "retain_eplusout_msgpack": True},
            "reporting_measures": [
                {
                    "measure_dir_name": "TestReportingMeasure1",
                    "arguments": {
                        "TestReportingMeasure1_arg1": "TestReportingMeasure1_val1",
                        "TestReportingMeasure1_arg2": "TestReportingMeasure1_val2",
                    },
                },
                {
                    "measure_dir_name": "TestReportingMeasure2",
                    "arguments": {
                        "TestReportingMeasure2_arg1": "TestReportingMeasure2_val1",
                        "TestReportingMeasure2_arg2": "TestReportingMeasure2_val2",
                    },
                },
            ],
            "measures": [
                {
                    "measure_dir_name": "TestMeasure1",
                    "arguments": {
                        "TestMeasure1_arg1": 1,
                        "TestMeasure1_arg2": 2,
                    },
                },
                {"measure_dir_name": "TestMeasure2"},
            ],
        },
    },
    "upgrades": [
        {
            "upgrade_name": "Upgrade 1",
            "options": [
                {
                    "option": "Parameter|Option",
                }
            ],
        }
    ],
}


def pytest_generate_tests(metafunc):
    # Generate various combinations of blocks in the configuration file
    # because the yaml file will not always contain all the blocks - it can be any subset of the blocks
    if "dynamic_cfg" in metafunc.fixturenames:
        arg_blocks = [
            "build_existing_model",
            "emissions",
            "utility_bills",
            "measures",
            "reporting_measures",
            "simulation_output_report",
            "server_directory_cleanup",
        ]
        blocks_to_remove = []
        for i in range(0, len(arg_blocks) + 1):
            blocks_to_remove.extend(list(itertools.combinations(arg_blocks, i)))

        cfg_variants = []
        for blocks in blocks_to_remove:
            cfg = copy.deepcopy(test_cfg)
            for block_name in blocks:
                del cfg["workflow_generator"]["args"][block_name]
            cfg_variants.append(cfg)

            # Add a variant without the add_component_loads key
            if "build_existing_model" not in blocks:
                cfg = copy.deepcopy(cfg)
                del cfg["workflow_generator"]["args"]["build_existing_model"]["add_component_loads"]
                cfg_variants.append(cfg)

            # Add a variant with only one emissions scenario
            if "emissions" not in blocks:
                cfg = copy.deepcopy(cfg)
                cfg["workflow_generator"]["args"]["emissions"].pop()
                cfg_variants.append(cfg)

            # Add a variant with only one utility bill scenario
            if "utility_bills" not in blocks:
                cfg = copy.deepcopy(cfg)
                cfg["workflow_generator"]["args"]["utility_bills"].pop()
                cfg_variants.append(cfg)

            # Add a variant with only one output_variable, and no output_variables key
            if "simulation_output_report" not in blocks:
                cfg = copy.deepcopy(cfg)
                cfg["workflow_generator"]["args"]["simulation_output_report"]["output_variables"].pop()
                cfg_variants.append(cfg)
                cfg = copy.deepcopy(cfg)
                del cfg["workflow_generator"]["args"]["simulation_output_report"]["output_variables"]
                cfg_variants.append(cfg)

        metafunc.parametrize("dynamic_cfg", cfg_variants)


@pytest.mark.parametrize("upgrade", [0, None])
def test_residential_hpxml(upgrade, dynamic_cfg):
    sim_id = "bldb1up1"
    building_id = 13
    n_datapoints = 10
    cfg = copy.deepcopy(dynamic_cfg)

    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade)

    index = 0

    build_existing_model_step = osw["steps"][index]
    assert build_existing_model_step["measure_dir_name"] == "BuildExistingModel"
    assert build_existing_model_step["arguments"]["building_id"] == building_id

    workflow_args = cfg["workflow_generator"].get("args", {})

    if "build_existing_model" in workflow_args:
        assert build_existing_model_step["arguments"]["simulation_control_timestep"] == 15
        assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_month"] == 2
        assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_day_of_month"] == 1
        assert build_existing_model_step["arguments"]["simulation_control_run_period_end_month"] == 2
        assert build_existing_model_step["arguments"]["simulation_control_run_period_end_day_of_month"] == 28
        assert build_existing_model_step["arguments"]["simulation_control_run_period_calendar_year"] == 2010
    else:
        # Defaults
        assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_month"] == 1
        assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_day_of_month"] == 1
        assert build_existing_model_step["arguments"]["simulation_control_run_period_end_month"] == 12
        assert build_existing_model_step["arguments"]["simulation_control_run_period_end_day_of_month"] == 31
        assert build_existing_model_step["arguments"]["simulation_control_run_period_calendar_year"] == 2007
        assert build_existing_model_step["arguments"]["simulation_control_timestep"] == 60

    if "emissions" in workflow_args:

        assert build_existing_model_step["arguments"]["emissions_scenario_names"] == ",".join(
            e["scenario_name"] for e in workflow_args["emissions"]
        )
        assert build_existing_model_step["arguments"]["emissions_natural_gas_values"] == ",".join(
            str(e["gas_value"]) for e in workflow_args["emissions"]
        )

    if "utility_bills" in workflow_args:
        assert build_existing_model_step["arguments"]["utility_bill_scenario_names"] == ",".join(
            u["scenario_name"] for u in workflow_args["utility_bills"]
        )
        assert build_existing_model_step["arguments"]["utility_bill_natural_gas_fixed_charges"] == ",".join(
            str(u.get("gas_fixed_charge", "")) for u in workflow_args["utility_bills"]
        )
        assert build_existing_model_step["arguments"]["utility_bill_simple_filepaths"] == ",".join(
            u.get("simple_filepath", "") for u in workflow_args["utility_bills"]
        )
    index += 1

    if upgrade is not None:
        apply_upgrade_step = osw["steps"][index]
        assert apply_upgrade_step["measure_dir_name"] == "ApplyUpgrade"
        assert apply_upgrade_step["arguments"]["upgrade_name"] == "Upgrade 1"
        assert apply_upgrade_step["arguments"]["run_measure"] == 1
        assert apply_upgrade_step["arguments"]["option_1"] == "Parameter|Option"
        index += 1

    hpxml_to_os_step = osw["steps"][index]
    assert hpxml_to_os_step["measure_dir_name"] == "HPXMLtoOpenStudio"
    if "build_existing_model" in workflow_args:
        assert hpxml_to_os_step["arguments"]["add_component_loads"] == workflow_args["build_existing_model"].get(
            "add_component_loads", False
        )
    else:
        assert hpxml_to_os_step["arguments"]["add_component_loads"] is False
    assert hpxml_to_os_step["arguments"]["debug"] is True
    index += 1

    upgrade_costs_step = osw["steps"][index]
    assert upgrade_costs_step["measure_dir_name"] == "UpgradeCosts"
    assert upgrade_costs_step["arguments"]["debug"] is True
    index += 1

    if "measures" in workflow_args:
        assert osw["steps"][index]["measure_dir_name"] == "TestMeasure1"
        assert osw["steps"][index]["arguments"]["TestMeasure1_arg1"] == 1
        assert osw["steps"][index]["arguments"]["TestMeasure1_arg2"] == 2
        index += 1

        assert osw["steps"][index]["measure_dir_name"] == "TestMeasure2"
        assert osw["steps"][index].get("arguments") is None
        index += 1

    simulation_output_step = osw["steps"][index]
    assert simulation_output_step["measure_dir_name"] == "ReportSimulationOutput"
    if "simulation_output_report" in workflow_args:
        assert simulation_output_step["arguments"]["timeseries_frequency"] == "hourly"
        assert simulation_output_step["arguments"]["include_timeseries_total_consumptions"] is True
        assert simulation_output_step["arguments"]["include_timeseries_end_use_consumptions"] is True
        assert simulation_output_step["arguments"]["include_timeseries_total_loads"] is True
        assert simulation_output_step["arguments"]["include_timeseries_zone_temperatures"] is True
        if "output_variables" in workflow_args["simulation_output_report"]:
            assert simulation_output_step["arguments"]["user_output_variables"] == ",".join(
                v["name"] for v in workflow_args["simulation_output_report"]["output_variables"]
            )
    else:  # Defaults
        assert simulation_output_step["arguments"]["timeseries_frequency"] == "none"
        assert simulation_output_step["arguments"]["include_timeseries_total_consumptions"] is False
        assert simulation_output_step["arguments"]["include_timeseries_end_use_consumptions"] is True
        assert simulation_output_step["arguments"]["include_timeseries_total_loads"] is True
        assert simulation_output_step["arguments"]["include_timeseries_zone_temperatures"] is False
        assert simulation_output_step["arguments"]["user_output_variables"] == ""

    assert simulation_output_step["arguments"]["include_annual_total_consumptions"] is True
    assert simulation_output_step["arguments"]["include_annual_fuel_consumptions"] is True
    assert simulation_output_step["arguments"]["include_annual_end_use_consumptions"] is True
    assert simulation_output_step["arguments"]["include_annual_system_use_consumptions"] is False
    assert simulation_output_step["arguments"]["include_annual_emissions"] is True
    assert simulation_output_step["arguments"]["include_annual_emission_fuels"] is True
    assert simulation_output_step["arguments"]["include_annual_emission_end_uses"] is True
    assert simulation_output_step["arguments"]["include_annual_total_loads"] is True
    assert simulation_output_step["arguments"]["include_annual_unmet_hours"] is True
    assert simulation_output_step["arguments"]["include_annual_peak_fuels"] is True
    assert simulation_output_step["arguments"]["include_annual_peak_loads"] is True
    assert simulation_output_step["arguments"]["include_annual_component_loads"] is True
    assert simulation_output_step["arguments"]["include_annual_hot_water_uses"] is True
    assert simulation_output_step["arguments"]["include_annual_hvac_summary"] is True
    assert simulation_output_step["arguments"]["include_annual_resilience"] is True
    assert simulation_output_step["arguments"]["include_timeseries_fuel_consumptions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_system_use_consumptions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emissions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emission_fuels"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emission_end_uses"] is False
    assert simulation_output_step["arguments"]["include_timeseries_hot_water_uses"] is False
    assert simulation_output_step["arguments"]["include_timeseries_component_loads"] is False
    assert simulation_output_step["arguments"]["include_timeseries_unmet_hours"] is False
    assert simulation_output_step["arguments"]["include_timeseries_airflows"] is False
    assert simulation_output_step["arguments"]["include_timeseries_weather"] is False
    assert simulation_output_step["arguments"]["include_timeseries_resilience"] is False
    assert simulation_output_step["arguments"]["timeseries_timestamp_convention"] == "end"
    assert simulation_output_step["arguments"]["timeseries_num_decimal_places"] == 3
    assert simulation_output_step["arguments"]["add_timeseries_dst_column"] is True
    assert simulation_output_step["arguments"]["add_timeseries_utc_column"] is True
    index += 1

    utility_bills_step = osw["steps"][index]
    assert utility_bills_step["measure_dir_name"] == "ReportUtilityBills"
    assert utility_bills_step["arguments"]["include_annual_bills"] is False
    assert utility_bills_step["arguments"]["include_monthly_bills"] is False
    assert utility_bills_step["arguments"]["register_annual_bills"] is True
    if "simulation_output_report" in workflow_args:
        assert utility_bills_step["arguments"]["register_monthly_bills"] is True
    index += 1

    if "reporting_measures" in workflow_args:
        assert osw["steps"][index]["measure_dir_name"] == "TestReportingMeasure1"
        assert osw["steps"][index]["arguments"]["TestReportingMeasure1_arg1"] == "TestReportingMeasure1_val1"
        assert osw["steps"][index]["arguments"]["TestReportingMeasure1_arg2"] == "TestReportingMeasure1_val2"
        index += 1

        assert osw["steps"][index]["measure_dir_name"] == "TestReportingMeasure2"
        assert osw["steps"][index]["arguments"]["TestReportingMeasure2_arg1"] == "TestReportingMeasure2_val1"
        assert osw["steps"][index]["arguments"]["TestReportingMeasure2_arg2"] == "TestReportingMeasure2_val2"
        index += 1

    server_dir_cleanup_step = osw["steps"][index]
    assert server_dir_cleanup_step["measure_dir_name"] == "ServerDirectoryCleanup"
    assert server_dir_cleanup_step["arguments"]["debug"] is True
    if "server_directory_cleanup" in workflow_args:
        assert server_dir_cleanup_step["arguments"]["retain_in_osm"] is True
        assert server_dir_cleanup_step["arguments"]["retain_eplusout_msgpack"] is True
    else:  # Defaults
        assert server_dir_cleanup_step["arguments"]["retain_in_osm"] is False
        assert server_dir_cleanup_step["arguments"]["retain_eplusout_msgpack"] is False
    index += 1


def test_missing_arg_warning():
    """
    Some keys defined in schema can be unavailable in the measure.
    This test verifies that such keys are not passed to the measure, but warnings are raised.
    """
    cfg = {
        "buildstock_directory": resstock_directory,
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {
            "type": "residential_hpxml",
            "args": {
                "build_existing_model": {
                    "simulation_control_run_period_begin_month": 2,
                    "add_component_loads": True,
                    "new_key1": "test_value",  # simulate old resstock by adding a new key not in the measure
                },
            },
        },
    }
    n_datapoints = 10
    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)
    osw_gen.default_args["BuildExistingModel"]["new_key2"] = "test_value"

    with LogCapture(level=logging.INFO) as log:
        measure_args = osw_gen.create_osw("bldb1up1", 13, None)
        assert len(log.records) == 2
        all_msg = "\n".join([record.msg for record in log.records])
        assert "'new_key1' not found in 'BuildExistingModel'" in all_msg
        assert "'new_key2' not found in 'BuildExistingModel'" in all_msg
    assert "new_key1" not in measure_args
    assert "new_key2" not in measure_args

    del cfg["workflow_generator"]["args"]["build_existing_model"]["new_key1"]
    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)
    with LogCapture(level=logging.INFO) as log:
        measure_args = osw_gen.create_osw("bldb1up1", 13, None)
        assert len(log.records) == 0


def test_hpmxl_schema_defaults_and_mapping():
    """
    Verify that the keys used in the defaults, workflow schema and arg mapping are available in the measure
    """
    schema_yaml = os.path.join(os.path.dirname(__file__), "..", "residential_hpxml_schema.yml")
    schema_obj = yamale.make_schema(schema_yaml, parser="ruamel")

    def get_mapped_away_yaml_keys(yaml_block_name):
        mapped_keys = set()
        for measure_dir_name, measure_arg_maps in ARG_MAP.items():
            for block_name, measure_arg_map in measure_arg_maps.items():
                if block_name == yaml_block_name:
                    mapped_keys.update(measure_arg_map.keys())
        return mapped_keys

    def assert_valid_mapped_keys(measure_dir_name, valid_args):
        for yaml_block_name, measure_arg_map in ARG_MAP.get(measure_dir_name, {}).items():
            for measure_arg in measure_arg_map.values():
                assert measure_arg in valid_args, f"{measure_arg} in ARG_MAP[{yaml_block_name}] not available"
                f"as input to {measure_dir_name} measure"

    def assert_valid_schema_keys(yaml_block_name, valid_keys, exclude_keys):
        schema_keys = set(schema_obj.includes[schema_obj.dict[yaml_block_name].include_name].dict.keys())
        for key in schema_keys - exclude_keys:
            assert key in valid_keys, f"{key} in {yaml_block_name} not available as input to the measure"

    def assert_valid_default_keys(measure_dir_name, valid_keys, exclude_keys):
        default_keys = set(DEFAULT_MEASURE_ARGS[measure_dir_name].keys())
        for key in default_keys - exclude_keys:
            assert key in valid_keys, f"{key} in defaults not available in {measure_dir_name} measure"

    def assert_valid_keys(measure_dir_name, yaml_block_name):
        available_measure_input = ResidentialHpxmlWorkflowGenerator.get_measure_arguments_from_xml(
            resstock_directory, measure_dir_name
        )
        exclude_keys = get_mapped_away_yaml_keys(yaml_block_name)  # Keys that are mapped away to other measures
        assert_valid_default_keys(measure_dir_name, available_measure_input, exclude_keys)
        assert_valid_mapped_keys(measure_dir_name, available_measure_input)
        if yaml_block_name:  # Not all measures have input defined/allowed in the schema
            assert_valid_schema_keys(yaml_block_name, available_measure_input, exclude_keys)

    assert_valid_keys("BuildExistingModel", "build_existing_model")
    assert_valid_keys("HPXMLtoOpenStudio", None)
    assert_valid_keys("UpgradeCosts", None)
    assert_valid_keys("ReportSimulationOutput", "simulation_output_report")
    assert_valid_keys("ReportUtilityBills", None)
    assert_valid_keys("ServerDirectoryCleanup", "server_directory_cleanup")


def test_block_compression_and_argmap():
    test_wf_arg = {
        "block1": {"key1": "v1", "key2": "v2", "key3": ["v3", "v4"], "key4": "v4"},
        "block2": [
            {
                "key1": "val1",
                "key2": "val2",
            },
            {
                "key1": "val11",
                "key3": "val33",
            },
        ],
    }
    compressed_block = ResidentialHpxmlWorkflowGenerator._get_condensed_block(test_wf_arg["block1"])
    assert compressed_block == test_wf_arg["block1"]
    compressed_block = ResidentialHpxmlWorkflowGenerator._get_condensed_block(test_wf_arg["block2"])
    assert compressed_block == {"key1": ["val1", "val11"], "key2": ["val2", ""], "key3": ["", "val33"]}
    arg_map = {
        "measure1": {
            "block1": {
                # "key1": "arg1",  # key1 is passed to measure 2
                "key2": "arg2",
                "key3": "arg3",
                "key_not_in_block": "arg5",
                "key_not_in_block2": "arg6",
            }
        },
        "measure2": {
            "block2": {
                "key1": "arg1",
                "key2": "arg2",
                "key3": "arg3",
            },
            "block1": {"key1": "arg4"},
        },
    }
    measure_args = ResidentialHpxmlWorkflowGenerator._get_mapped_args_from_block(
        test_wf_arg["block1"], arg_map["measure1"]["block1"], {"arg6": False}
    )
    assert measure_args == {
        "arg2": "v2",
        "arg3": "v3,v4",
        "arg5": "",
        "arg6": False,
    }
    measure_args = ResidentialHpxmlWorkflowGenerator._get_mapped_args_from_block(
        test_wf_arg["block1"], arg_map["measure2"]["block1"], {}
    )
    assert measure_args == {
        "arg4": "v1",
    }

    measure_args = ResidentialHpxmlWorkflowGenerator._get_mapped_args_from_block(
        test_wf_arg["block2"], arg_map["measure2"]["block2"], {}
    )
    assert measure_args == {
        "arg1": "val1,val11",
        "arg2": "val2,",
        "arg3": ",val33",
    }

    # Only key4 should be remaining since the other three is already mapped to measure
    assert test_wf_arg["block1"] == {"key4": "v4"}
