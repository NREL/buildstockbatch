import csv
import dask
import glob
import json
import numpy as np
import os
import pandas as pd
from pyarrow import parquet
import pytest
import re
import shutil
import tempfile
from unittest.mock import patch, MagicMock
import yaml

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.postprocessing import write_dataframe_as_parquet

dask.config.set(scheduler='synchronous')
here = os.path.dirname(os.path.abspath(__file__))


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
    assert(not df['simulation_output_report.applicable'].any())


def test_reference_scenario(basic_residential_project_file):
    # verify that the reference_scenario get's added to the upgrade file

    upgrade_config = {
        'upgrades': [
            {
                'upgrade_name': 'Triple-Pane Windows',
                'reference_scenario': 'example_reference_scenario'
            }
        ]
    }
    project_filename, results_dir = basic_residential_project_file(upgrade_config)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    # test results.csv files
    test_path = os.path.join(results_dir, 'results_csvs')
    test_csv = pd.read_csv(os.path.join(test_path, 'results_up01.csv.gz'))
    assert len(test_csv['apply_upgrade.reference_scenario'].unique()) == 1
    assert test_csv['apply_upgrade.reference_scenario'].iloc[0] == 'example_reference_scenario'


def test_combine_files_flexible(basic_residential_project_file):
    # Allows addition/removable/rename of columns. For columns that remain unchanged, verifies that the data matches
    # with stored test_results. If this test passes but test_combine_files fails, then test_results/parquet and
    # test_results/results_csvs need to be updated with new data *if* columns were indeed supposed to be added/
    # removed/renamed.

    post_process_config = {
        'postprocessing': {
            'aggregate_timeseries': True
        }
    }
    project_filename, results_dir = basic_residential_project_file(post_process_config)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    def simplify_columns(colname):
        return colname.lower().replace('_', '')

    # test results.csv files
    reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'results_csvs')
    test_path = os.path.join(results_dir, 'results_csvs')

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up00.csv.gz')).rename(columns=simplify_columns).\
        sort_values('buildingid').reset_index().drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up00.csv.gz')).rename(columns=simplify_columns).\
        sort_values('buildingid').reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_csv.columns).intersection(set(reference_csv)))
    pd.testing.assert_frame_equal(test_csv[mutul_cols], reference_csv[mutul_cols])

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up01.csv.gz')).rename(columns=simplify_columns).\
        sort_values('buildingid').reset_index().drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up01.csv.gz')).rename(columns=simplify_columns).\
        sort_values('buildingid').reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_csv.columns).intersection(set(reference_csv)))
    pd.testing.assert_frame_equal(test_csv[mutul_cols], reference_csv[mutul_cols])

    # test parquet files
    reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'parquet')
    test_path = os.path.join(results_dir, 'parquet')

    # results parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'baseline', 'results_up00.parquet')).\
        rename(columns=simplify_columns).sort_values('buildingid').reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path, 'baseline', 'results_up00.parquet')).\
        rename(columns=simplify_columns).sort_values('buildingid').reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    test_pq = pd.read_parquet(os.path.join(test_path, 'upgrades', 'upgrade=1', 'results_up01.parquet')).\
        rename(columns=simplify_columns).sort_values('buildingid').reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'upgrades', 'upgrade=1', 'results_up01.parquet')).\
        rename(columns=simplify_columns).sort_values('buildingid').reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    # aggregated_timeseries parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'aggregated_timeseries',
                                           'upgrade=0', 'aggregated_ts_up00.parquet')).\
        rename(columns=simplify_columns).sort_values(['time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'aggregated_timeseries',
                                                'upgrade=0', 'aggregated_ts_up00.parquet')).\
        rename(columns=simplify_columns).sort_values(['time']).reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    test_pq = pd.read_parquet(os.path.join(test_path, 'aggregated_timeseries',
                                           'upgrade=1', 'aggregated_ts_up01.parquet')).\
        rename(columns=simplify_columns).sort_values(['time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'aggregated_timeseries',
                                                'upgrade=1', 'aggregated_ts_up01.parquet')).\
        rename(columns=simplify_columns).sort_values(['time']).reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    # timeseries parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=0', 'Group0.parquet')).\
        rename(columns=simplify_columns).sort_values(['buildingid', 'time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=0', 'Group0.parquet')).\
        rename(columns=simplify_columns).sort_values(['buildingid', 'time']).reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    test_pq = pd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=1', 'Group0.parquet')).\
        rename(columns=simplify_columns).sort_values(['buildingid', 'time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=1', 'Group0.parquet')).\
        rename(columns=simplify_columns).sort_values(['buildingid', 'time']).reset_index().drop(columns=['index'])
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])


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

        # Test n_datapoints do not match
        with open(project_filename, 'r') as f:
            cfg = yaml.safe_load(f)
        cfg['baseline']['n_datapoints'] = 100
        with open(project_filename, 'w') as f:
            yaml.dump(cfg, f)

        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir):
            with pytest.raises(RuntimeError) as ex:
                bsb = BuildStockBatchBase(project_filename)
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
            with pytest.raises(FileNotFoundError) as ex:
                bsb = BuildStockBatchBase(project_filename)

        # Test downselect mutually exclusive
        with open(project_filename, 'r') as f:
            cfg = yaml.safe_load(f)
        cfg['baseline']['buildstock_csv'] = buildstock_csv
        cfg['downselect'] = {'resample': True, 'logic': []}
        with open(project_filename, 'w') as f:
            yaml.dump(cfg, f)

        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir):
            with pytest.raises(RuntimeError) as ex:
                bsb = BuildStockBatchBase(project_filename)
            assert('Remove or comment out the downselect key' in str(ex.value))


def test_downselect_integer_options(basic_residential_project_file):
    with tempfile.TemporaryDirectory() as buildstock_csv_dir:
        buildstock_csv = os.path.join(buildstock_csv_dir, 'buildstock.csv')
        valid_option_values = set()
        with open(os.path.join(here, 'buildstock.csv'), 'r', newline='') as f_in, \
                open(buildstock_csv, 'w', newline='') as f_out:
            cf_in = csv.reader(f_in)
            cf_out = csv.writer(f_out)
            for i, row in enumerate(cf_in):
                if i == 0:
                    col_idx = row.index('Days Shifted')
                else:
                    # Convert values from "Day1" to "1.10" so we hit the bug
                    row[col_idx] = '{0}.{0}0'.format(re.search(r'Day(\d+)', row[col_idx]).group(1))
                    valid_option_values.add(row[col_idx])
                cf_out.writerow(row)

        project_filename, results_dir = basic_residential_project_file({
            'downselect': {
                'resample': False,
                'logic': 'Geometry House Size|1500-2499'
            }
        })
        run_sampling_mock = MagicMock(return_value=buildstock_csv)
        with patch.object(BuildStockBatchBase, 'weather_dir', None), \
                patch.object(BuildStockBatchBase, 'results_dir', results_dir), \
                patch.object(BuildStockBatchBase, 'run_sampling', run_sampling_mock):
            bsb = BuildStockBatchBase(project_filename)
            bsb.downselect()
            run_sampling_mock.assert_called_once()
            with open(buildstock_csv, 'r', newline='') as f:
                cf = csv.DictReader(f)
                for row in cf:
                    assert(row['Days Shifted'] in valid_option_values)


def test_combine_files(basic_residential_project_file):

    post_process_config = {
        'postprocessing': {
            'aggregate_timeseries': True
        }
    }
    project_filename, results_dir = basic_residential_project_file(post_process_config)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    # test results.csv files
    reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'results_csvs')
    test_path = os.path.join(results_dir, 'results_csvs')

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up00.csv.gz')).sort_values('building_id').reset_index()\
        .drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up00.csv.gz')).sort_values('building_id')\
        .reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_csv, reference_csv)

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up01.csv.gz')).sort_values('building_id').reset_index()\
        .drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up01.csv.gz')).sort_values('building_id')\
        .reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_csv, reference_csv)

    # test parquet files
    reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'parquet')
    test_path = os.path.join(results_dir, 'parquet')

    # results parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'baseline', 'results_up00.parquet')).sort_values('building_id')\
        .reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path, 'baseline', 'results_up00.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    test_pq = pd.read_parquet(os.path.join(test_path, 'upgrades', 'upgrade=1', 'results_up01.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'upgrades', 'upgrade=1', 'results_up01.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    # aggregated_timeseries parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'aggregated_timeseries',
                                           'upgrade=0', 'aggregated_ts_up00.parquet')).\
        sort_values(['Time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'aggregated_timeseries',
                                                'upgrade=0', 'aggregated_ts_up00.parquet')).\
        sort_values(['Time']).reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    test_pq = pd.read_parquet(os.path.join(test_path, 'aggregated_timeseries',
                                           'upgrade=1', 'aggregated_ts_up01.parquet')).\
        sort_values(['Time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'aggregated_timeseries',
                                                'upgrade=1', 'aggregated_ts_up01.parquet')).\
        sort_values(['Time']).reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    # timeseries parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=0', 'Group0.parquet')).\
        sort_values(['building_id', 'time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=0', 'Group0.parquet'))\
        .sort_values(['building_id', 'time']).reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    test_pq = pd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=1', 'Group0.parquet'))\
        .sort_values(['building_id', 'time']).reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=1', 'Group0.parquet'))\
        .sort_values(['building_id', 'time']).reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)


@patch('buildstockbatch.postprocessing.boto3')
def test_upload_files(mocked_s3, basic_residential_project_file):
    s3_bucket = 'test_bucket'
    s3_prefix = 'test_prefix'
    s3_folder = 'test_folder'
    db_name = 'test_db_name'
    role = 'test_role'
    region = 'test_region'

    upload_config = {
                    'postprocessing': {
                        'aws': {
                            'region_name': region,
                            's3': {
                                'bucket': s3_bucket,
                                'prefix': s3_prefix,
                                'upload_folder': s3_folder,
                                    },
                            'athena': {
                                'glue_service_role': role,
                                'database_name': db_name,
                                'max_crawling_time': 250
                                    }
                            }
                        }
                    }
    mocked_glueclient = MagicMock()
    mocked_glueclient.get_crawler = MagicMock(return_value={'Crawler': {'State': 'READY'}})
    mocked_s3.client = MagicMock(return_value=mocked_glueclient)
    project_filename, results_dir = basic_residential_project_file(upload_config)
    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'output_dir', results_dir), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    files_uploaded = []
    crawler_created = False
    crawler_started = False
    for call in mocked_s3.mock_calls + mocked_s3.client().mock_calls:
        call_function = call[0].split('.')[-1]  # 0 is for the function name
        if call_function == 'resource':
            assert call[1][0] in ['s3']  # call[1] is for the positional arguments
        if call_function == 'Bucket':
            assert call[1][0] == s3_bucket
        if call_function == 'upload_file':
            source_file_path = call[1][0]
            destination_path = call[1][1]
            files_uploaded.append((source_file_path, destination_path))
        if call_function == 'create_crawler':
            crawler_para = call[2]  # 2 is for the keyboard arguments
            crawler_created = True
            assert crawler_para['DatabaseName'] == upload_config['postprocessing']['aws']['athena']['database_name']
            assert crawler_para['Role'] == upload_config['postprocessing']['aws']['athena']['glue_service_role']
            assert crawler_para['TablePrefix'] == s3_folder + '_'
            assert crawler_para['Name'] == db_name + '_' + s3_folder
            assert crawler_para['Targets']['S3Targets'][0]['Path'] == 's3://' + s3_bucket + '/' + s3_prefix + '/' + \
                                                                      s3_folder + '/'
        if call_function == 'start_crawler':
            assert crawler_created, "crawler attempted to start before creating"
            crawler_started = True
            crawler_para = call[2]  # 2 is for keyboard arguments.
            assert crawler_para['Name'] == db_name + '_' + s3_folder

    assert crawler_started, "Crawler never started"

    # check if all the files are properly uploaded
    source_path = os.path.join(results_dir, 'parquet')
    s3_path = s3_prefix + '/' + s3_folder + '/'

    s3_file_path = s3_path + 'baseline/results_up00.parquet'
    source_file_path = os.path.join(source_path, 'baseline', 'results_up00.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'upgrades/upgrade=1/results_up01.parquet'
    source_file_path = os.path.join(source_path, 'upgrades', 'upgrade=1', 'results_up01.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'timeseries/upgrade=0/Group0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=0', 'Group0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'timeseries/upgrade=1/Group0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=1', 'Group0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    assert len(files_uploaded) == 0, f"These files shouldn't have been uploaded: {files_uploaded}"


def test_write_parquet_no_index():
    df = pd.DataFrame(np.random.randn(6, 4), columns=list('abcd'), index=np.arange(6))

    with tempfile.TemporaryDirectory() as tmpdir:
        filename = 'df.parquet'
        write_dataframe_as_parquet(df, tmpdir, filename)
        schema = parquet.read_schema(os.path.join(tmpdir, filename))
        assert '__index_level_0__' not in schema.names
        assert df.columns.values.tolist() == schema.names


def test_skipping_baseline(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file({
        'baseline': {
            'skip_sims': True
        }
    })

    sim_output_path = os.path.join(results_dir, 'simulation_output')
    assert 'up00' in os.listdir(sim_output_path)
    baseline_path = os.path.join(sim_output_path, 'up00')
    shutil.rmtree(baseline_path)
    assert 'up00' not in os.listdir(sim_output_path)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):

        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    up00_parquet = os.path.join(results_dir, 'parquet', 'upgrades', 'upgrade=0', 'results_up00.parquet')
    assert(not os.path.exists(up00_parquet))

    up01_parquet = os.path.join(results_dir, 'parquet', 'upgrades', 'upgrade=1', 'results_up01.parquet')
    assert(os.path.exists(up01_parquet))

    up00_csv_gz = os.path.join(results_dir, 'results_csvs', 'results_up00.csv.gz')
    assert(not os.path.exists(up00_csv_gz))

    up01_csv_gz = os.path.join(results_dir, 'results_csvs', 'results_up01.csv.gz')
    assert(os.path.exists(up01_csv_gz))


def test_report_additional_results_csv_columns(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file({
        'reporting_measures': [
            'ReportingMeasure1',
            'ReportingMeasure2'
        ]
    })

    for filename in glob.glob(os.path.join(results_dir, 'simulation_output', 'up*', 'bldg*', 'run',
                                           'data_point_out.json')):
        with open(filename, 'r') as f:
            dpout = json.load(f)
        dpout['ReportingMeasure1'] = {'column_1': 1, 'column_2': 2}
        dpout['ReportingMeasure2'] = {'column_3': 3, 'column_4': 4}
        with open(filename, 'w') as f:
            json.dump(dpout, f)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):

        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    up00_results_csv_path = os.path.join(results_dir, 'results_csvs', 'results_up00.csv.gz')
    up00 = pd.read_csv(up00_results_csv_path)
    assert 'reporting_measure1' in [col.split('.')[0] for col in up00.columns]
    assert 'reporting_measure2' in [col.split('.')[0] for col in up00.columns]

    up01_results_csv_path = os.path.join(results_dir, 'results_csvs', 'results_up01.csv.gz')
    up01 = pd.read_csv(up01_results_csv_path)
    assert 'reporting_measure1' in [col.split('.')[0] for col in up01.columns]
    assert 'reporting_measure2' in [col.split('.')[0] for col in up01.columns]
