import csv
import dask
import dask.dataframe as dd
from fsspec.implementations.local import LocalFileSystem
import gzip
import json
import numpy as np
import os
import pandas as pd
from pyarrow import parquet
import re
import shutil
import tempfile
from unittest.mock import patch, MagicMock

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.postprocessing import write_dataframe_as_parquet

dask.config.set(scheduler='synchronous')
here = os.path.dirname(os.path.abspath(__file__))

OUTPUT_FOLDER_NAME = 'output'


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
    test_csv = pd.read_csv(os.path.join(test_path, 'results_up01.csv.gz')).set_index('building_id').sort_index()
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

    # timeseries parquet
    test_pq = dd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=0'), engine='pyarrow')\
        .compute().reset_index()
    reference_pq = dd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=0'), engine='pyarrow')\
        .compute().reset_index()
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])

    test_pq = dd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=1'), engine='pyarrow')\
        .compute().reset_index()
    reference_pq = dd.read_parquet(os.path.join(reference_path,  'timeseries', 'upgrade=1'), engine='pyarrow')\
        .compute().reset_index()
    mutul_cols = list(set(test_pq.columns).intersection(set(reference_pq)))
    pd.testing.assert_frame_equal(test_pq[mutul_cols], reference_pq[mutul_cols])


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
                    col_idx = row.index('Bathroom Spot Vent Hour')
                else:
                    # Convert values from "Hour1" to "1.10" so we hit the bug
                    row[col_idx] = '{0}.{0}0'.format(re.search(r'Hour(\d+)', row[col_idx]).group(1))
                    valid_option_values.add(row[col_idx])
                cf_out.writerow(row)

        project_filename, results_dir = basic_residential_project_file({
            'downselect': {
                'resample': False,
                'logic': 'Geometry Floor Area Bin|1500-2499'
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
                    assert(row['Bathroom Spot Vent Hour'] in valid_option_values)


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

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up00.csv.gz'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up00.csv.gz'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_csv, reference_csv)

    test_csv = pd.read_csv(os.path.join(test_path, 'results_up01.csv.gz'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    reference_csv = pd.read_csv(os.path.join(reference_path, 'results_up01.csv.gz'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_csv, reference_csv)

    # test parquet files
    reference_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_results', 'parquet')
    test_path = os.path.join(results_dir, 'parquet')

    # results parquet
    test_pq = pd.read_parquet(os.path.join(test_path, 'baseline', 'results_up00.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path, 'baseline', 'results_up00.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    test_pq = pd.read_parquet(os.path.join(test_path, 'upgrades', 'upgrade=1', 'results_up01.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    reference_pq = pd.read_parquet(os.path.join(reference_path,  'upgrades', 'upgrade=1', 'results_up01.parquet'))\
        .sort_values('building_id').reset_index().drop(columns=['index'])
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    # timeseries parquet
    test_pq = dd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=0'), engine='pyarrow')\
        .compute().reset_index()
    reference_pq = dd.read_parquet(os.path.join(reference_path, 'timeseries', 'upgrade=0'), engine='pyarrow')\
        .compute().reset_index()
    pd.testing.assert_frame_equal(test_pq, reference_pq)

    test_pq = dd.read_parquet(os.path.join(test_path, 'timeseries', 'upgrade=1'), engine='pyarrow')\
        .compute().reset_index()
    reference_pq = dd.read_parquet(os.path.join(reference_path, 'timeseries', 'upgrade=1'), engine='pyarrow')\
        .compute().reset_index()
    pd.testing.assert_frame_equal(test_pq, reference_pq)


@patch('buildstockbatch.postprocessing.boto3')
def test_upload_files(mocked_s3, basic_residential_project_file):
    s3_bucket = 'test_bucket'
    s3_prefix = 'test_prefix'
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
            assert crawler_para['TablePrefix'] == OUTPUT_FOLDER_NAME + '_'
            assert crawler_para['Name'] == db_name + '_' + OUTPUT_FOLDER_NAME
            assert crawler_para['Targets']['S3Targets'][0]['Path'] == 's3://' + s3_bucket + '/' + s3_prefix + '/' + \
                                                                      OUTPUT_FOLDER_NAME + '/'
        if call_function == 'start_crawler':
            assert crawler_created, "crawler attempted to start before creating"
            crawler_started = True
            crawler_para = call[2]  # 2 is for keyboard arguments.
            assert crawler_para['Name'] == db_name + '_' + OUTPUT_FOLDER_NAME

    assert crawler_started, "Crawler never started"

    # check if all the files are properly uploaded
    source_path = os.path.join(results_dir, 'parquet')
    s3_path = s3_prefix + '/' + OUTPUT_FOLDER_NAME + '/'

    s3_file_path = s3_path + 'baseline/results_up00.parquet'
    source_file_path = os.path.join(source_path, 'baseline', 'results_up00.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'upgrades/upgrade=1/results_up01.parquet'
    source_file_path = os.path.join(source_path, 'upgrades', 'upgrade=1', 'results_up01.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'timeseries/upgrade=0/part.0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=0', 'part.0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'timeseries/upgrade=1/part.0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=1', 'part.0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    assert len(files_uploaded) == 0, f"These files shouldn't have been uploaded: {files_uploaded}"


def test_write_parquet_no_index():
    df = pd.DataFrame(np.random.randn(6, 4), columns=list('abcd'), index=np.arange(6))

    with tempfile.TemporaryDirectory() as tmpdir:
        fs = LocalFileSystem()
        filename = os.path.join(tmpdir, 'df.parquet')
        write_dataframe_as_parquet(df, fs, filename)
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
    shutil.rmtree(os.path.join(sim_output_path, 'timeseries', 'up00'))
    results_json_filename = os.path.join(sim_output_path, 'results_job0.json.gz')
    with gzip.open(results_json_filename, 'rt', encoding='utf-8') as f:
        dpouts = json.load(f)
    dpouts2 = list(filter(lambda x: x['upgrade'] > 0, dpouts))
    with gzip.open(results_json_filename, 'wt', encoding='utf-8') as f:
        json.dump(dpouts2, f)

    with patch.object(BuildStockBatchBase, 'weather_dir', None), \
            patch.object(BuildStockBatchBase, 'get_dask_client') as get_dask_client_mock, \
            patch.object(BuildStockBatchBase, 'results_dir', results_dir):

        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    up00_parquet = os.path.join(results_dir, 'parquet', 'baseline', 'results_up00.parquet')
    assert(not os.path.exists(up00_parquet))

    up01_parquet = os.path.join(results_dir, 'parquet', 'upgrades', 'upgrade=1', 'results_up01.parquet')
    assert(os.path.exists(up01_parquet))

    up00_csv_gz = os.path.join(results_dir, 'results_csvs', 'results_up00.csv.gz')
    assert(not os.path.exists(up00_csv_gz))

    up01_csv_gz = os.path.join(results_dir, 'results_csvs', 'results_up01.csv.gz')
    assert(os.path.exists(up01_csv_gz))
