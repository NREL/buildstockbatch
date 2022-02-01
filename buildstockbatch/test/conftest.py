import os
import pytest
import shutil
import tempfile
import yaml

OUTPUT_FOLDER_NAME = 'output'


@pytest.fixture
def basic_residential_project_file():
    with tempfile.TemporaryDirectory() as test_directory:
        def _basic_residential_project_file(update_args={}, raw=False):
            output_dir = "simulations_job0" if raw else "simulation_output"
            buildstock_directory = os.path.join(test_directory, 'openstudio_buildstock')
            shutil.copytree(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_inputs', 'test_openstudio_buildstock'),
                buildstock_directory
            )
            project_directory = 'project_resstock_national'
            os.makedirs(os.path.join(buildstock_directory, project_directory))
            output_directory = os.path.join(test_directory, OUTPUT_FOLDER_NAME)
            shutil.copytree(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', output_dir),
                os.path.join(output_directory, 'simulation_output')
            )

            # move the job*.json file to appropriate location
            if os.path.exists(os.path.join(output_directory, 'simulation_output', 'job0.json')):
                shutil.move(os.path.join(output_directory, 'simulation_output', 'job0.json'),
                            os.path.join(output_directory, 'simulation_output', '..', '..', 'job0.json'))

            os.mkdir(os.path.join(output_directory, 'housing_characteristics'))
            os.mkdir(os.path.join(buildstock_directory, project_directory, 'housing_characteristics'))
            cfg = {
                'buildstock_directory': buildstock_directory,
                'project_directory': project_directory,
                'output_directory': output_directory,
                'weather_files_url': 'https://s3.amazonaws.com/epwweatherfiles/project_resstock_national.zip',
                'sampler': {
                    'type': 'residential_quota',
                    'args': {
                        'n_datapoints': 8
                    }
                },
                'workflow_generator': {
                    'type': 'residential_default',
                    'args': {
                        'timeseries_csv_export': {
                            'reporting_frequency': 'Hourly',
                            'include_enduse_subcategories': True
                        },
                        'simulation_output': {
                            'include_enduse_subcategories': True
                        },
                    }
                },
                'baseline': {
                    'n_buildings_represented': 80000000,
                },
                'upgrades': [{
                    'upgrade_name': 'Upgrade1',
                    'options': [
                        {'option': 'Infiltration|11.25 ACH50'}
                    ]
                            }],
                'eagle': {
                    'sampling': {
                        'time': 20
                    },
                    'account': 'testaccount',
                },
                'schema_version': '0.3'
            }
            cfg.update(update_args)
            project_filename = os.path.join(test_directory, 'project.yml')
            with open(project_filename, 'w') as f:
                yaml.dump(cfg, f)
            return project_filename, output_directory

        yield _basic_residential_project_file
