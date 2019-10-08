import os
import pytest
import shutil
import tempfile
import yaml

OUTPUT_FOLDER_NAME = 'output'


@pytest.fixture
def basic_residential_project_file():
    with tempfile.TemporaryDirectory() as test_directory:
        buildstock_directory = os.path.join(test_directory, 'openstudio_buildstock')
        shutil.copytree(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_inputs', 'test_openstudio_buildstock'),
            os.path.join(buildstock_directory)
        )
        project_directory = 'project_resstock_national'
        os.makedirs(os.path.join(buildstock_directory, project_directory))
        output_directory = os.path.join(test_directory, OUTPUT_FOLDER_NAME)
        shutil.copytree(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'simulation_output'),
            os.path.join(output_directory, 'simulation_output')
        )

        def _basic_residential_project_file(update_args={}):
            cfg = {
                'stock_type': 'residential',
                'buildstock_directory': buildstock_directory,
                'project_directory': project_directory,
                'output_directory': output_directory,
                'weather_files_url': 'https://s3.amazonaws.com/epwweatherfiles/project_resstock_national.zip',
                'baseline': {
                    'n_datapoints': 10,
                    'n_buildings_represented': 80000000
                },
                'timeseries_csv_export': {
                    'reporting_frequency': 'Hourly',
                    'include_enduse_subcategories': True
                },
                'eagle': {
                    'n_jobs': 20,
                    'sampling': {
                        'time': 20
                    },
                    'account': 'testaccount',
                }
            }
            cfg.update(update_args)
            project_filename = os.path.join(test_directory, 'project.yml')
            with open(project_filename, 'w') as f:
                yaml.dump(cfg, f)
            return project_filename, output_directory

        yield _basic_residential_project_file
