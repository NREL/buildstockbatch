import dask
import json
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
import shutil
import tempfile
import yaml

from buildstockbatch.base import BuildStockBatchBase


dask.config.set(scheduler='synchronous')
here = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def basic_residential_project_file():
    with tempfile.TemporaryDirectory() as test_directory:

        buildstock_directory = os.path.join(test_directory, 'openstudio_buildstock')
        project_directory = 'project_resstock_national'
        os.makedirs(os.path.join(buildstock_directory, project_directory))
        output_directory = os.path.join(test_directory, 'output')
        shutil.copytree(
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results'),
            os.path.join(output_directory, 'simulation_output')
        )

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

    # Modify the results to remove the simulation output report from all of one upgrade.
    simout_dir = os.path.join(results_dir, 'simulation_output')
    for upgrade_dir in os.listdir(simout_dir):
        full_upgrade_dir = os.path.join(simout_dir, upgrade_dir)
        if not os.path.isdir(full_upgrade_dir):
            continue
        for bldg_dir in os.listdir(full_upgrade_dir):
            datapoint_out_filename = os.path.join(simout_dir, upgrade_dir, bldg_dir, 'run', 'data_point_out.json')
            if upgrade_dir.endswith('up01') and os.path.isfile(datapoint_out_filename):
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
        get_dask_client_mock.assert_called_once()

    up01_parquet = os.path.join(results_dir, 'parquet', 'upgrades', 'upgrade=1', 'results_up01.parquet')
    assert(os.path.exists(up01_parquet))
    df = pd.read_parquet(up01_parquet, engine='pyarrow')
    assert((~df['simulation_output_report.applicable']).any())


def test_provide_buildstock_csv(basic_residential_project_file):
    with tempfile.TemporaryDirectory() as buildstock_csv_dir:
        buildstock_csv = os.path.join(buildstock_csv_dir, 'buildstock.csv')
        df = pd.read_csv(os.path.join(here, 'buildstock.csv'))
        df2 = df[df['Vintage'] == '<1950']
        df2.to_csv(buildstock_csv, index=False)

        project_filename, results_dir = basic_residential_project_file({
            'baseline': {
                'n_buildings_represented': 80000000,
                'buildstock_csv': buildstock_csv
            }
        })
        sampler = MagicMock()
        copied_csv = os.path.join(buildstock_csv_dir, 'buildstock_out.csv')
        sampler.csv_path = copied_csv
        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir):
            bsb = BuildStockBatchBase(project_filename)
            bsb.sampler = sampler
            sampling_output_csv = bsb.run_sampling()
            assert(sampling_output_csv == copied_csv)
            df3 = pd.read_csv(sampling_output_csv)
            assert(df3.shape == df2.shape)
            assert((df3['Vintage'] == '<1950').all())

        # Test mutually exclusive failure
        with open(project_filename, 'r') as f:
            cfg = yaml.safe_load(f)
        cfg['baseline']['n_datapoints'] = 10
        with open(project_filename, 'w') as f:
            yaml.dump(cfg, f)

        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir):
            bsb = BuildStockBatchBase(project_filename)
            bsb.sampler = sampler
            with pytest.raises(RuntimeError) as ex:
                bsb.run_sampling()
            assert('n_datapoints for sampling should not be provided' in str(ex.value))

        # Test file missing
        with open(project_filename, 'r') as f:
            cfg = yaml.safe_load(f)
        del cfg['baseline']['n_datapoints']
        cfg['baseline']['buildstock_csv'] = os.path.join(buildstock_csv_dir, 'non_existant_file.csv')
        with open(project_filename, 'w') as f:
            yaml.dump(cfg, f)

        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir):
            bsb = BuildStockBatchBase(project_filename)
            bsb.sampler = sampler
            with pytest.raises(FileNotFoundError) as ex:
                bsb.run_sampling()
