import dask
import json
import os
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
import tempfile
import yaml

from buildstockbatch.base import BuildStockBatchBase

dask.config.set(scheduler='synchronous')
here = os.path.dirname(os.path.abspath(__file__))

OUTPUT_FOLDER_NAME = 'output'


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


def test_combine_files_flexible(basic_residential_project_file):
    # Allows addition/removable/rename of columns. For columns that remain unchanged, verifies that the data matches
    # with stored test_results. If this test passes but test_combine_files fails, then test_results/parquet and
    # test_results/results_csvs need to be updated with new data *if* columns were indeed supposed to be added/
    # removed/renamed.

    project_filename, results_dir = basic_residential_project_file()

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


def test_combine_files(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()

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


@patch('buildstockbatch.base.boto3')
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

    s3_file_path = s3_path + 'timeseries/upgrade=0/Group0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=0', 'Group0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + 'timeseries/upgrade=1/Group0.parquet'
    source_file_path = os.path.join(source_path, 'timeseries', 'upgrade=1', 'Group0.parquet')
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    assert len(files_uploaded) == 0, f"These files shouldn't have been uploaded: {files_uploaded}"
