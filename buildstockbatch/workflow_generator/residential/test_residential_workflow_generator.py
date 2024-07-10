from buildstockbatch.workflow_generator.residential.residential_hpxml import (
    ResidentialHpxmlWorkflowGenerator,
)
from buildstockbatch.workflow_generator.residential.residential_hpxml_defaults import DEFAULT_MEASURE_ARGS
from buildstockbatch.workflow_generator.residential.residential_hpxml_arg_mapping import ARG_MAP
from testfixtures import LogCapture
from buildstockbatch.test.shared_testing_stuff import resstock_directory
import os
import yamale
import logging


def test_residential_hpxml(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    cfg = {
        "buildstock_directory": resstock_directory,
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {
            "type": "residential_hpxml",
            "args": {
                "build_existing_model": {
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
                    "include_timeseries_zone_temperatures": False,
                    "output_variables": [
                        {"name": "Zone Mean Air Temperature"},
                        {"name": "Zone People Occupant Count"},
                    ],
                },
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
    n_datapoints = 10
    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)

    osw = osw_gen.create_osw(sim_id, building_id, 0)
    assert len(osw["steps"]) == 12

    apply_upgrade_step = osw["steps"][1]
    assert apply_upgrade_step["measure_dir_name"] == "ApplyUpgrade"
    assert apply_upgrade_step["arguments"]["upgrade_name"] == "Upgrade 1"
    assert apply_upgrade_step["arguments"]["run_measure"] == 1
    assert apply_upgrade_step["arguments"]["option_1"] == "Parameter|Option"

    build_existing_model_step = osw["steps"][0]
    assert build_existing_model_step["measure_dir_name"] == "BuildExistingModel"
    assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_month"] == 2
    assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_day_of_month"] == 1
    assert build_existing_model_step["arguments"]["simulation_control_run_period_end_month"] == 2
    assert build_existing_model_step["arguments"]["simulation_control_run_period_end_day_of_month"] == 28
    assert build_existing_model_step["arguments"]["simulation_control_run_period_calendar_year"] == 2010

    assert build_existing_model_step["arguments"]["emissions_scenario_names"] == "LRMER_MidCase_15,LRMER_HighCase_15"
    assert build_existing_model_step["arguments"]["emissions_natural_gas_values"] == "147.3,187.3"

    assert build_existing_model_step["arguments"]["utility_bill_scenario_names"] == "Bills,Bills2"
    assert build_existing_model_step["arguments"]["utility_bill_natural_gas_fixed_charges"] == ",12.0"
    assert build_existing_model_step["arguments"]["utility_bill_simple_filepaths"] == ","

    hpxml_to_os_step = osw["steps"][2]
    assert hpxml_to_os_step["measure_dir_name"] == "HPXMLtoOpenStudio"

    assert osw["steps"][3]["measure_dir_name"] == "TestMeasure1"
    assert osw["steps"][3]["arguments"]["TestMeasure1_arg1"] == 1
    assert osw["steps"][3]["arguments"]["TestMeasure1_arg2"] == 2
    assert osw["steps"][4]["measure_dir_name"] == "TestMeasure2"
    assert osw["steps"][4].get("arguments") is None

    simulation_output_step = osw["steps"][5]
    assert simulation_output_step["measure_dir_name"] == "ReportSimulationOutput"
    assert simulation_output_step["arguments"]["timeseries_frequency"] == "hourly"
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
    assert simulation_output_step["arguments"]["include_timeseries_total_consumptions"] is True
    assert simulation_output_step["arguments"]["include_timeseries_fuel_consumptions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_end_use_consumptions"] is True
    assert simulation_output_step["arguments"]["include_timeseries_system_use_consumptions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emissions"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emission_fuels"] is False
    assert simulation_output_step["arguments"]["include_timeseries_emission_end_uses"] is False
    assert simulation_output_step["arguments"]["include_timeseries_hot_water_uses"] is False
    assert simulation_output_step["arguments"]["include_timeseries_total_loads"] is True
    assert simulation_output_step["arguments"]["include_timeseries_component_loads"] is False
    assert simulation_output_step["arguments"]["include_timeseries_unmet_hours"] is False
    assert simulation_output_step["arguments"]["include_timeseries_zone_temperatures"] is False
    assert simulation_output_step["arguments"]["include_timeseries_airflows"] is False
    assert simulation_output_step["arguments"]["include_timeseries_weather"] is False
    assert simulation_output_step["arguments"]["include_timeseries_resilience"] is False
    assert simulation_output_step["arguments"]["timeseries_timestamp_convention"] == "end"
    assert simulation_output_step["arguments"]["timeseries_num_decimal_places"] == 3
    assert simulation_output_step["arguments"]["add_timeseries_dst_column"] is True
    assert simulation_output_step["arguments"]["add_timeseries_utc_column"] is True

    hpxml_output_step = osw["steps"][6]
    assert hpxml_output_step["measure_dir_name"] == "ReportHPXMLOutput"

    utility_bills_step = osw["steps"][7]
    assert utility_bills_step["measure_dir_name"] == "ReportUtilityBills"
    assert utility_bills_step["arguments"]["include_annual_bills"] is True
    assert utility_bills_step["arguments"]["include_monthly_bills"] is False

    upgrade_costs_step = osw["steps"][8]
    assert upgrade_costs_step["measure_dir_name"] == "UpgradeCosts"

    assert osw["steps"][9]["measure_dir_name"] == "TestReportingMeasure1"
    assert osw["steps"][9]["arguments"]["TestReportingMeasure1_arg1"] == "TestReportingMeasure1_val1"
    assert osw["steps"][9]["arguments"]["TestReportingMeasure1_arg2"] == "TestReportingMeasure1_val2"
    assert osw["steps"][10]["measure_dir_name"] == "TestReportingMeasure2"
    assert osw["steps"][10]["arguments"]["TestReportingMeasure2_arg1"] == "TestReportingMeasure2_val1"
    assert osw["steps"][10]["arguments"]["TestReportingMeasure2_arg2"] == "TestReportingMeasure2_val2"

    server_dir_cleanup_step = osw["steps"][11]
    assert server_dir_cleanup_step["measure_dir_name"] == "ServerDirectoryCleanup"


def test_old_resstock(mocker):
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
                    "new_key1": "test_value",  # simulate old resstock by adding a new key not in the measure
                },
            },
        },
    }
    n_datapoints = 10
    osw_gen = ResidentialHpxmlWorkflowGenerator(cfg, n_datapoints)
    osw_gen.default_args["BuildExistingModel"]["new_key2"] = "test_value"

    with LogCapture(level=logging.INFO) as log:
        measure_args = osw_gen._get_measure_args(
            cfg["workflow_generator"]["args"]["build_existing_model"], "BuildExistingModel", debug=False
        )
        assert len(log.records) == 2
        all_msg = "\n".join([record.msg for record in log.records])
        assert "'new_key1' in workflow_generator not found in 'BuildExistingModel'" in all_msg
        assert "'new_key2' in defaults not found in 'BuildExistingModel'" in all_msg
    assert "new_key1" not in measure_args
    assert "new_key2" not in measure_args


def test_hpmxl_schema_defaults_and_mapping():
    """
    Verify that the keys used in the defaults, workflow schema and arg mapping are available in the measure
    """
    schema_yaml = os.path.join(os.path.dirname(__file__), "residential_hpxml_schema.yml")
    schema_obj = yamale.make_schema(schema_yaml, parser="ruamel")

    def assert_valid_mapped_keys(measure_dir_name, valid_args):
        for yaml_block_name, measure_arg_map in ARG_MAP.items():
            for measure_arg in measure_arg_map.get(measure_dir_name, {}).values():
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

    def assert_valid_keys(measure_dir_name, yaml_block_name, exclude_keys):
        avilable_measure_input = ResidentialHpxmlWorkflowGenerator.get_measure_arguments_from_xml(
            resstock_directory, measure_dir_name
        )
        assert_valid_default_keys(measure_dir_name, avilable_measure_input, exclude_keys)
        assert_valid_mapped_keys(measure_dir_name, avilable_measure_input)
        if yaml_block_name:  # Not all measures have input defined/allowed in the schema
            assert_valid_schema_keys(yaml_block_name, avilable_measure_input, exclude_keys)

    assert_valid_keys("BuildExistingModel", "build_existing_model", {"add_component_loads"})
    assert_valid_keys("HPXMLtoOpenStudio", None, set())
    assert_valid_keys("ReportSimulationOutput", "simulation_output_report", {"output_variables"})
    assert_valid_keys("ReportHPXMLOutput", None, set())
    assert_valid_keys("ReportUtilityBills", None, set())
    assert_valid_keys("UpgradeCosts", None, set())
    assert_valid_keys("ServerDirectoryCleanup", "server_directory_cleanup", set())


def test_block_compression_and_argmap():
    test_wf_arg = {
        "block1": {"key1": "v1", "key2": "v2", "key3": ["v3", "v4"]},
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
        "block1": {
            "measure1": {
                # "key1": "arg1",  # key1 is not to be passed
                "key2": "arg2",
                "key3": "arg3",
            },
        },
        "block2": {
            "measure2": {
                "key1": "arg1",
                "key2": "arg2",
                "key3": "arg3",
            },
        },
    }
    measure_args = ResidentialHpxmlWorkflowGenerator._get_mapped_args_from_block(
        test_wf_arg["block1"], arg_map["block1"]
    )
    assert measure_args == {
        "measure1": {
            "arg2": "v2",
            "arg3": "v3,v4",
        }
    }
    measure_args = ResidentialHpxmlWorkflowGenerator._get_mapped_args_from_block(
        test_wf_arg["block2"], arg_map["block2"]
    )
    assert measure_args == {
        "measure2": {
            "arg1": "val1,val11",
            "arg2": "val2,",
            "arg3": ",val33",
        }
    }

    # Only key1 should be remaining since the other two is already mapped to measure
    assert test_wf_arg["block1"] == {"key1": "v1"}
