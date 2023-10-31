from buildstockbatch.workflow_generator.base import WorkflowGeneratorBase
from buildstockbatch.workflow_generator.residential_hpxml import (
    ResidentialHpxmlWorkflowGenerator,
)
from buildstockbatch.workflow_generator.commercial import (
    CommercialDefaultWorkflowGenerator,
)
from buildstockbatch.test.shared_testing_stuff import resstock_directory


def test_apply_logic_recursion():
    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(["one", "two", "three"])
    assert apply_logic == "(one&&two&&three)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"and": ["one", "two", "three"]})
    assert apply_logic == "(one&&two&&three)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"or": ["four", "five", "six"]})
    assert apply_logic == "(four||five||six)"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg({"not": "seven"})
    assert apply_logic == "!seven"

    apply_logic = WorkflowGeneratorBase.make_apply_logic_arg(
        {"and": [{"not": "abc"}, {"or": ["def", "ghi"]}, "jkl", "mno"]}
    )
    assert apply_logic == "(!abc&&(def||ghi)&&jkl&&mno)"


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
                },
                "simulation_output_report": {
                    "timeseries_frequency": "hourly",
                    "include_timeseries_total_consumptions": True,
                    "include_timeseries_end_use_consumptions": True,
                    "include_timeseries_total_loads": True,
                    "include_timeseries_zone_temperatures": False,
                },
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
    assert len(steps) == 8

    build_existing_model_step = steps[0]
    assert build_existing_model_step["measure_dir_name"] == "BuildExistingModel"
    assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_month"] == 2
    assert build_existing_model_step["arguments"]["simulation_control_run_period_begin_day_of_month"] == 1
    assert build_existing_model_step["arguments"]["simulation_control_run_period_end_month"] == 2
    assert build_existing_model_step["arguments"]["simulation_control_run_period_end_day_of_month"] == 28
    assert build_existing_model_step["arguments"]["simulation_control_run_period_calendar_year"] == 2010

    apply_upgrade_step = steps[1]
    assert apply_upgrade_step["measure_dir_name"] == "ApplyUpgrade"

    hpxml_to_os_step = steps[2]
    assert hpxml_to_os_step["measure_dir_name"] == "HPXMLtoOpenStudio"

    simulation_output_step = steps[3]
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

    hpxml_output_step = steps[4]
    assert hpxml_output_step["measure_dir_name"] == "ReportHPXMLOutput"

    utility_bills_step = steps[5]
    assert utility_bills_step["measure_dir_name"] == "ReportUtilityBills"
    assert utility_bills_step["arguments"]["include_annual_bills"] is True
    assert utility_bills_step["arguments"]["include_monthly_bills"] is False

    upgrade_costs_step = steps[6]
    assert upgrade_costs_step["measure_dir_name"] == "UpgradeCosts"

    server_dir_cleanup_step = steps[7]
    assert server_dir_cleanup_step["measure_dir_name"] == "ServerDirectoryCleanup"


def test_com_default_workflow_generator_basic(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    upgrade_idx = None
    cfg = {
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {"type": "commercial_default", "args": {}},
    }
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get BuildExistingModel
    reporting_measure_step = osw["steps"][0]
    assert reporting_measure_step["measure_dir_name"] == "BuildExistingModel"
    assert reporting_measure_step["arguments"]["number_of_buildings_represented"] == 1
    assert reporting_measure_step["measure_type"] == "ModelMeasure"
    # Should not get TimeseriesCSVExport if excluded in args
    assert len(osw["steps"]) == 1


def test_com_default_workflow_generator_with_timeseries(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    upgrade_idx = None
    cfg = {
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {
            "type": "commercial_default",
            "args": {
                "timeseries_csv_export": {
                    "reporting_frequency": "Hourly",
                    "inc_output_variables": "true",
                }
            },
        },
    }
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get BuildExistingModel
    reporting_measure_step = osw["steps"][0]
    assert reporting_measure_step["measure_dir_name"] == "BuildExistingModel"
    assert reporting_measure_step["arguments"]["number_of_buildings_represented"] == 1
    assert reporting_measure_step["measure_type"] == "ModelMeasure"
    # Should get TimeseriesCSVExport if included in args
    reporting_measure_step = osw["steps"][1]
    assert reporting_measure_step["measure_dir_name"] == "TimeseriesCSVExport"
    assert reporting_measure_step["measure_type"] == "ReportingMeasure"
    assert reporting_measure_step["arguments"]["reporting_frequency"] == "Hourly"
    assert reporting_measure_step["arguments"]["inc_output_variables"] == "true"


def test_com_default_workflow_generator_extended(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    upgrade_idx = None
    cfg = {
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {
            "type": "commercial_default",
            "args": {
                "reporting_measures": [
                    {
                        "measure_dir_name": "f8e23017-894d-4bdf-977f-37e3961e6f42",
                        "arguments": {
                            "building_summary_section": "true",
                            "annual_overview_section": "true",
                            "monthly_overview_section": "true",
                            "utility_bills_rates_section": "true",
                            "envelope_section_section": "true",
                            "space_type_breakdown_section": "true",
                            "space_type_details_section": "true",
                            "interior_lighting_section": "true",
                            "plug_loads_section": "true",
                            "exterior_light_section": "true",
                            "water_use_section": "true",
                            "hvac_load_profile": "true",
                            "zone_condition_section": "true",
                            "zone_summary_section": "true",
                            "zone_equipment_detail_section": "true",
                            "air_loops_detail_section": "true",
                            "plant_loops_detail_section": "true",
                            "outdoor_air_section": "true",
                            "cost_summary_section": "true",
                            "source_energy_section": "true",
                            "schedules_overview_section": "true",
                        },
                    },
                    {"measure_dir_name": "SimulationOutputReport"},
                    {"measure_dir_name": "comstock_sensitivity_reports"},
                    {"measure_dir_name": "qoi_report"},
                    {
                        "measure_dir_name": "la_100_qaqc",
                        "arguments": {"run_qaqc": "true"},
                    },
                    {
                        "measure_dir_name": "simulation_settings_check",
                        "arguments": {"run_sim_settings_checks": "true"},
                    },
                    {"measure_dir_name": "run_directory_cleanup"},
                ],
                "timeseries_csv_export": {
                    "reporting_frequency": "Hourly",
                    "inc_output_variables": "true",
                },
            },
        },
    }

    CommercialDefaultWorkflowGenerator.validate(cfg)
    CommercialDefaultWorkflowGenerator.validate(cfg)
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw = osw_gen.create_osw(sim_id, building_id, upgrade_idx)

    # Should always get SimulationOutputReport
    reporting_measure_step = osw["steps"][3]
    assert reporting_measure_step["measure_dir_name"] == "SimulationOutputReport"
    assert reporting_measure_step["measure_type"] == "ReportingMeasure"
    assert reporting_measure_step["arguments"] == {}
    # Should only be one instance of SimulationOutputReport
    assert [d["measure_dir_name"] == "SimulationOutputReport" for d in osw["steps"]].count(True) == 1
    # Should get TimeseriesCSVExport if included in args
    reporting_measure_step = osw["steps"][1]
    assert reporting_measure_step["measure_dir_name"] == "TimeseriesCSVExport"
    assert reporting_measure_step["measure_type"] == "ReportingMeasure"
    assert reporting_measure_step["arguments"]["reporting_frequency"] == "Hourly"
    assert reporting_measure_step["arguments"]["inc_output_variables"] == "true"
    # Should have the openstudio report
    reporting_measure_step = osw["steps"][2]
    assert reporting_measure_step["measure_dir_name"] == "f8e23017-894d-4bdf-977f-37e3961e6f42"
    assert reporting_measure_step["measure_type"] == "ReportingMeasure"
    assert reporting_measure_step["arguments"]["building_summary_section"] == "true"
    assert reporting_measure_step["arguments"]["schedules_overview_section"] == "true"
    # Should have 1 workflow measure plus 9 reporting measures
    assert len(osw["steps"]) == 9
