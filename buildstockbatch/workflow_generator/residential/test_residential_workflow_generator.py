from buildstockbatch.workflow_generator.residential.residential_hpxml import (
    ResidentialHpxmlWorkflowGenerator, get_measure_arguments
)
from buildstockbatch.workflow_generator.residential.residential_hpxml_defaults import (
    DEFAULT_MEASURE_ARGS
)
from buildstockbatch.workflow_generator.residential.residential_hpxml_arg_mapping import (
    BUILD_EXISTING_MODEL_ARG_MAP
)

from buildstockbatch.test.shared_testing_stuff import resstock_directory
import os
import yamale


def test_residential_hpxml(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    upgrade_idx = 0
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
                    "add_component_loads": True
                },
                "emissions": [
                    {
                        "scenario_name": "LRMER_MidCase_15",
                        "type": "CO2e",
                        "elec_folder": "data/emissions/cambium/2022/LRMER_MidCase_15",
                        "gas_value": 147.3,
                        "propane_value": 177.8,
                        "oil_value": 195.9,
                        "wood_value": 200.0
                    },
                    {
                        "scenario_name": "LRMER_HighCase_15",
                        "type": "CO2e",
                        "elec_folder": "data/emissions/cambium/2022/LRMER_HighCase_15",
                        "gas_value": 187.3,
                        "propane_value": 187.8,
                        "oil_value": 199.9,
                        "wood_value": 250.0
                    }
                ],
                "utility_bills": [
                    {
                        "scenario_name": "Bills",
                        "elc_fixed_charge": 10.0,
                        "elc_marginal_rate": 0.12
                    },
                    {
                        "scenario_name": "Bills2",
                        "gas_fixed_charge": 12.0,
                        "gas_marginal_rate": 0.15
                    }
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
                    ]
                },
                "reporting_measures": [
                    {
                        "measure_dir_name": "TestReportingMeasure1",
                        "arguments": {
                            "TestReportingMeasure1_arg1": "TestReportingMeasure1_val1",
                            "TestReportingMeasure1_arg2": "TestReportingMeasure1_val2",
                        }
                    },
                    {
                        "measure_dir_name": "TestReportingMeasure2",
                        "arguments": {
                            "TestReportingMeasure2_arg1": "TestReportingMeasure2_val1",
                            "TestReportingMeasure2_arg2": "TestReportingMeasure2_val2",
                        }
                    },
                ],
                "measures": [
                    {
                        "measure_dir_name": "TestMeasure1",
                        "arguments": {
                            "TestMeasure1_arg1": 1,
                            "TestMeasure1_arg2": 2,
                        }
                    },
                    {"measure_dir_name": "TestMeasure2"},
                ]
            },
        },
        "upgrades": [
            {
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
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    steps = osw["steps"]
    assert len(steps) == 12

    build_existing_model_step = steps[0]
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
    apply_upgrade_step = steps[1]
    assert apply_upgrade_step["measure_dir_name"] == "ApplyUpgrade"

    hpxml_to_os_step = steps[2]
    assert hpxml_to_os_step["measure_dir_name"] == "HPXMLtoOpenStudio"

    assert steps[3]["measure_dir_name"] == "TestMeasure1"
    assert steps[3]["arguments"]["TestMeasure1_arg1"] == 1
    assert steps[3]["arguments"]["TestMeasure1_arg2"] == 2
    assert steps[4]["measure_dir_name"] == "TestMeasure2"
    assert steps[4].get("arguments") is None

    simulation_output_step = steps[5]
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

    hpxml_output_step = steps[6]
    assert hpxml_output_step["measure_dir_name"] == "ReportHPXMLOutput"

    utility_bills_step = steps[7]
    assert utility_bills_step["measure_dir_name"] == "ReportUtilityBills"
    assert utility_bills_step["arguments"]["include_annual_bills"] is True
    assert utility_bills_step["arguments"]["include_monthly_bills"] is False

    upgrade_costs_step = steps[8]
    assert upgrade_costs_step["measure_dir_name"] == "UpgradeCosts"

    assert steps[9]["measure_dir_name"] == "TestReportingMeasure1"
    assert steps[9]["arguments"]["TestReportingMeasure1_arg1"] == "TestReportingMeasure1_val1"
    assert steps[9]["arguments"]["TestReportingMeasure1_arg2"] == "TestReportingMeasure1_val2"
    assert steps[10]["measure_dir_name"] == "TestReportingMeasure2"
    assert steps[10]["arguments"]["TestReportingMeasure2_arg1"] == "TestReportingMeasure2_val1"
    assert steps[10]["arguments"]["TestReportingMeasure2_arg2"] == "TestReportingMeasure2_val2"

    server_dir_cleanup_step = steps[11]
    assert server_dir_cleanup_step["measure_dir_name"] == "ServerDirectoryCleanup"


def test_hpmxl_schema_defaults_and_mapping():
    # load the residential_hpxml_schema.yml  in current directory
    schema_yaml = os.path.join(os.path.dirname(__file__), "residential_hpxml_schema.yml")
    schema_obj = yamale.make_schema(schema_yaml, parser="ruamel")

    # check build_existing_model keys are valid input to the measure
    measure_dir = os.path.join(resstock_directory, "measures")
    measure_path = os.path.join(measure_dir, "BuildExistingModel")
    avilable_measure_input = get_measure_arguments(os.path.join(measure_path, "measure.xml"))

    schema_keys = set(schema_obj.includes[schema_obj.dict["build_existing_model"].include_name].dict.keys())
    default_keys = set(DEFAULT_MEASURE_ARGS["measures/BuildExistingModel"].keys())
    emissions_keys = BUILD_EXISTING_MODEL_ARG_MAP["emissions"].values()  # dict values correspond to measure input
    utility_bills_keys = BUILD_EXISTING_MODEL_ARG_MAP["utility_bills"].values()
    special_keys = {"add_component_loads"}  # Special keys are not directly passed to the measure
    for key in schema_keys - special_keys:
        assert key in avilable_measure_input, f"{key} in schema not available as input to BuildExistingModel measure"
    for key in default_keys - special_keys:
        assert key in avilable_measure_input, f"{key} in defaults not available in BuildExistingModel measure"
    for key in emissions_keys:
        assert key in avilable_measure_input, f"{key} in BUILD_EXISTING_MODEL_ARG_MAP for emission invalid."
    for key in utility_bills_keys:
        assert key in avilable_measure_input, f"{key} in BUILD_EXISTING_MODEL_ARG_MAP for utility bills invalid."

    # check server_directory_cleanup keys are valid input to the measure
    measure_path = os.path.join(measure_dir, "ServerDirectoryCleanup")
    server_dir_cleanup_args_avail = get_measure_arguments(os.path.join(measure_path, "measure.xml"))
    schema_keys = set(schema_obj.includes[schema_obj.dict["server_directory_cleanup"].include_name].dict.keys())
    default_keys = set(DEFAULT_MEASURE_ARGS["measures/ServerDirectoryCleanup"].keys())
    for key in schema_keys:
        assert key in server_dir_cleanup_args_avail, f"{key} in schema not available as input to ServerDirectoryCleanup"
    for key in default_keys:
        assert key in schema_keys, f"{key} in defaults not available in ServerDirectoryCleanup"

    # check simulation_output_report keys are valid input to the measure
    measures_dir = os.path.join(resstock_directory, "resources/hpxml-measures")
    measure_path = os.path.join(measures_dir, "ReportSimulationOutput")
    sim_out_rep_args_avail = get_measure_arguments(os.path.join(measure_path, "measure.xml"))
    sim_ouput_schema_keys = schema_obj.includes[schema_obj.dict["simulation_output_report"].include_name].dict.keys()
    default_keys = DEFAULT_MEASURE_ARGS["resources/hpxml-measures/ReportSimulationOutput"].keys()
    special_keys = {"output_variables"}
    for key in sim_ouput_schema_keys - special_keys:
        assert key in sim_out_rep_args_avail, f"{key} in schema not available as input to ReportSimulationOutput"
    for key in default_keys - special_keys:
        assert key in sim_out_rep_args_avail, f"{key} in defaults not available as input to ReportSimulationOutput"
