import os
import pytest
import shutil
import tempfile
import yaml
from pathlib import Path

OUTPUT_FOLDER_NAME = "output"


@pytest.fixture
def basic_residential_project_file():
    with tempfile.TemporaryDirectory() as test_directory:

        def _basic_residential_project_file(update_args={}, raw=False):
            output_dir = "simulations_job0" if raw else "simulation_output"
            buildstock_directory = os.path.join(test_directory, "openstudio_buildstock")
            shutil.copytree(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "test_inputs",
                    "test_openstudio_buildstock",
                ),
                buildstock_directory,
            )
            project_directory = "project_resstock_national"
            os.makedirs(os.path.join(buildstock_directory, project_directory))
            output_directory = os.path.join(test_directory, OUTPUT_FOLDER_NAME)
            shutil.copytree(
                os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "test_results",
                    output_dir,
                ),
                os.path.join(output_directory, "simulation_output"),
            )

            # move the job*.json file to appropriate location
            if os.path.exists(os.path.join(output_directory, "simulation_output", "job0.json")):
                shutil.move(
                    os.path.join(output_directory, "simulation_output", "job0.json"),
                    os.path.join(output_directory, "simulation_output", "..", "..", "job0.json"),
                )

            os.mkdir(os.path.join(output_directory, "housing_characteristics"))
            os.mkdir(os.path.join(buildstock_directory, project_directory, "housing_characteristics"))
            cfg = {
                "buildstock_directory": buildstock_directory,
                "project_directory": project_directory,
                "output_directory": output_directory,
                "weather_files_url": "https://s3.amazonaws.com/epwweatherfiles/project_resstock_national.zip",
                "sampler": {"type": "residential_quota", "args": {"n_datapoints": 8}},
                "workflow_generator": {
                    "type": "residential_hpxml",
                    "args": {
                        "build_existing_model": {
                            "simulation_control_timestep": 60,
                            "simulation_control_run_period_begin_month": 1,
                            "simulation_control_run_period_begin_day_of_month": 1,
                            "simulation_control_run_period_end_month": 12,
                            "simulation_control_run_period_end_day_of_month": 31,
                            "simulation_control_run_period_calendar_year": 2007,
                        },
                        "emissions": [
                            {
                                "scenario_name": "LRMER_MidCase_15",
                                "type": "CO2e",
                                "elec_folder": "data/cambium/LRMER_MidCase_15",
                            }
                        ],
                        "utility_bills": [{"scenario_name": "Bills"}],
                        "simulation_output_report": {
                            "timeseries_frequency": "hourly",
                            "include_timeseries_total_consumptions": True,
                            "include_timeseries_fuel_consumptions": True,
                            "include_timeseries_end_use_consumptions": True,
                            "include_timeseries_emissions": True,
                            "include_timeseries_emission_fuels": True,
                            "include_timeseries_emission_end_uses": True,
                            "include_timeseries_hot_water_uses": True,
                            "include_timeseries_total_loads": True,
                            "include_timeseries_component_loads": True,
                            "include_timeseries_unmet_hours": True,
                            "include_timeseries_zone_temperatures": True,
                            "include_timeseries_airflows": True,
                            "include_timeseries_weather": True,
                        },
                        "reporting_measures": [],
                        "server_directory_cleanup": {
                            "retain_in_idf": False,
                            "retain_schedules_csv": False,
                        },
                    },
                },
                "baseline": {
                    "n_buildings_represented": 80000000,
                },
                "upgrades": [
                    {
                        "upgrade_name": "Upgrade1",
                        "options": [{"option": "Infiltration|11.25 ACH50"}],
                    }
                ],
                "eagle": {
                    "sampling": {"time": 20},
                    "account": "testaccount",
                    "minutes_per_sim": 1,
                },
                "schema_version": "0.3",
            }
            cfg.update(update_args)
            project_filename = os.path.join(test_directory, "project.yml")
            with open(project_filename, "w") as f:
                yaml.dump(cfg, f)
            return project_filename, output_directory

        yield _basic_residential_project_file
