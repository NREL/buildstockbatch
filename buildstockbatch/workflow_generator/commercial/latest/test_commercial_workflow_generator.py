from buildstockbatch.workflow_generator.commercial.latest.commercial import (
    CommercialDefaultWorkflowGenerator,
)


def test_com_default_workflow_generator_basic(mocker):
    sim_id = "bldb1up1"
    building_id = 1
    upgrade_idx = None
    cfg = {
        "baseline": {"n_buildings_represented": 100},
        "workflow_generator": {"type": "commercial_default", "args": {}},
    }
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw_gen.validate()
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
    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw_gen.validate()
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

    osw_gen = CommercialDefaultWorkflowGenerator(cfg, 10)
    osw_gen.validate()
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
