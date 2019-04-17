import json
import os
from unittest.mock import patch
import pytest
import shutil
import tempfile
import yaml

from buildstockbatch.base import BuildStockBatchBase


@pytest.fixture
def basic_residential_project_file():
    with tempfile.TemporaryDirectory() as test_directory:

        buildstock_directory = os.path.join(test_directory, 'openstudio_buildstock')
        project_directory = 'project_resstock_national'
        os.makedirs(os.path.join(buildstock_directory, project_directory))
        output_directory = os.path.join(test_directory, 'output')
        os.makedirs(output_directory)

        def _basic_residential_project_file(update_args={}):
            cfg = {
                'stock_type': 'residential',
                'buildstock_directory': buildstock_directory,
                'project_directory': project_directory,
                'ouput_directory': output_directory,
                'weather_files_url': 'https://s3.amazonaws.com/epwweatherfiles/project_resstock_national.zip',
                'baseline': {
                    'n_datapoints': 10,
                    'n_buildings_represented': 80000000
                }
            }
            cfg.update(update_args)
            project_filename = os.path.join(test_directory, 'project.yml')
            with open(project_filename, 'w') as f:
                yaml.dump(cfg, f)
            return project_filename, output_directory

        yield _basic_residential_project_file


def test_missing_simulation_output_report_applicable(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()
    os.rmdir(results_dir)
    shutil.copytree(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results'), results_dir)
    for item in os.listdir(results_dir):
        datapoint_out_filename = os.path.join(results_dir, item, 'run', 'data_point_out.json')
        if item.endswith('up01') and os.path.isfile(datapoint_out_filename):
            with open(datapoint_out_filename, 'r') as f:
                dpout = json.load(f)
            del dpout['SimulationOutputReport']
            with open(datapoint_out_filename, 'w') as f:
                json.dump(dpout, f)
    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
         patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
         patch.object(BuildStockBatchBase, 'results_dir', results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
