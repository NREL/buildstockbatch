# -*- coding: utf-8 -*-

"""
buildstockbatch.postprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A module containing utility functions for postprocessing

:author: Noel Merket, Rajendra Adhikari
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from collections import defaultdict
import dask.bag as db
import dask
import datetime as dt
from functools import partial
import gzip
import json
import logging
import random
import re
import sys
import time
import boto3
import pandas as pd
from pathlib import Path
import pyarrow as pa
from pyarrow import parquet

logger = logging.getLogger(__name__)


def divide_chunks(l, n):
    """
    Divide a list into chunks of size n
    """
    for i in range(0, len(l), n):
        yield l[i:i + n]


def read_data_point_out_json(fs, reporting_measures, filename):
    try:
        with fs.open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    else:
        if 'SimulationOutputReport' not in d:
            d['SimulationOutputReport'] = {'applicable': False}
        for reporting_measure in reporting_measures:
            if reporting_measure not in d:
                d[reporting_measure] = {'applicable': False}
        return d


def to_camelcase(x):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def flatten_datapoint_json(reporting_measures, d):
    new_d = {}
    cols_to_keep = {
        'ApplyUpgrade': [
            'upgrade_name',
            'applicable'
        ]
    }
    for k1, k2s in cols_to_keep.items():
        for k2 in k2s:
            new_d[f'{k1}.{k2}'] = d.get(k1, {}).get(k2)

    # copy over all the key and values from BuildExistingModel
    col1 = 'BuildExistingModel'
    for k, v in d.get(col1, {}).items():
        new_d[f'{col1}.{k}'] = v

    # if there is no units_represented key, default to 1
    units = int(new_d.get(f'{col1}.units_represented', 1))
    new_d[f'{col1}.units_represented'] = units
    col2 = 'SimulationOutputReport'
    for k, v in d.get(col2, {}).items():
        new_d[f'{col2}.{k}'] = v

    # additional reporting measures
    for col in reporting_measures:
        for k, v in d.get(col, {}).items():
            new_d[f'{col}.{k}'] = v

    new_d['building_unit_id'] = new_d['BuildExistingModel.building_unit_id']
    del new_d['BuildExistingModel.building_unit_id']

    return new_d


def read_out_osw(fs, filename):
    try:
        with fs.open(filename, 'r') as f:
            d = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    else:
        out_d = {}
        keys_to_copy = [
            'started_at',
            'completed_at',
            'completed_status'
        ]
        for key in keys_to_copy:
            out_d[key] = d.get(key, None)
        for step in d.get('steps', []):
            if step['measure_dir_name'] == 'BuildExistingModel':
                out_d['building_unit_id'] = step['arguments']['building_unit_id']
        return out_d


def write_dataframe_as_parquet(df, fs, filename):
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(filename, 'wb') as f:
        parquet.write_table(tbl, f, flavor='spark')


def add_timeseries(fs, results_dir, inp1, inp2):

    def get_factor(folder):
        path = f"{folder}/run/data_point_out.json"
        with fs.open(path, 'r') as f:
            js = json.load(f)
        units_represented = float(js['BuildExistingModel'].get('units_represented', 1))
        weight = float(js['BuildExistingModel']['weight'])
        factor = weight / units_represented
        return factor

    if type(inp1) is str:
        full_path = f"{inp1}/run/enduse_timeseries.parquet"
        try:
            with fs.open(full_path, 'rb') as f:
                file1 = pd.read_parquet(f, engine='pyarrow').set_index('Time')
                file1 = file1 * get_factor(inp1)
        except FileNotFoundError:
            file1 = pd.DataFrame()  # if the timeseries file is missing, set it to empty dataframe
    else:
        file1 = inp1

    if type(inp2) is str:
        full_path = f"{inp2}/run/enduse_timeseries.parquet"
        try:
            with fs.open(full_path, 'rb') as f:
                file2 = pd.read_parquet(f, engine='pyarrow').set_index('Time')
                file2 = file2 * get_factor(inp2)
        except FileNotFoundError:
            file2 = pd.DataFrame()
    else:
        file2 = inp2

    return file1.add(file2, fill_value=0)


def write_output(fs, results_dir, args):
    upgrade_id, group_id, folders = args

    folder_path = f"{results_dir}/parquet/timeseries/upgrade={upgrade_id}"
    fs.makedirs(folder_path, exist_ok=True)

    file_path = f"{folder_path}/Group{group_id}.parquet"
    parquets = []
    for folder in folders:
        full_path = f"{folder}/run/enduse_timeseries.parquet"
        if not fs.isfile(full_path):
            continue
        with fs.open(full_path, 'rb') as f:
            new_pq = pd.read_parquet(f, engine='pyarrow')
        new_pq.rename(columns=to_camelcase, inplace=True)

        building_unit_id_match = re.search(r'bldg(\d+)', folder)
        assert building_unit_id_match, f"The building results folder format should be: ~bldg(\\d+). Got: {folder} "
        building_unit_id = int(building_unit_id_match.group(1))
        new_pq['building_unit_id'] = building_unit_id
        parquets.append(new_pq)

    if not parquets:  # if no valid simulation is found for this group
        logger.warning(f'No valid simulation found for upgrade:{upgrade_id} and group:{group_id}.')
        logger.debug(f'The following folders were scanned {folders}.')
        return

    pq_size = (sum([sys.getsizeof(pq) for pq in parquets]) + sys.getsizeof(parquets)) / (1024 * 1024)
    logger.debug(f"Group{group_id}: list of {len(parquets)} parquets is consuming "
                 f"{pq_size:.2f} MB memory on a dask worker process.")
    pq = pd.concat(parquets)
    logger.debug(f"The concatenated parquet file is consuming {sys.getsizeof(pq) / (1024 * 1024) :.2f} MB.")
    write_dataframe_as_parquet(pq, fs, file_path)


def combine_results(fs, results_dir, config, skip_timeseries=False, aggregate_timeseries=False,
                    reporting_measures=[], dask_bag_partition_size=500):

    sim_out_dir = f'{results_dir}/simulation_output'
    results_csvs_dir = f'{results_dir}/results_csvs'
    parquet_dir = f'{results_dir}/parquet'
    ts_dir = f'{results_dir}/parquet/timeseries'
    agg_ts_dir = f'{results_dir}/parquet/aggregated_timeseries'
    dirs = [results_csvs_dir, parquet_dir]
    if not skip_timeseries:
        dirs.append(ts_dir)
        if aggregate_timeseries:
            dirs.append(agg_ts_dir)

    # clear and create the postprocessing results directories
    for dr in dirs:
        if fs.exists(dr):
            fs.rm(dr, recursive=True)
        fs.makedirs(dr)

    all_dirs = list()  # get the list of all the building simulation results directories
    results_by_upgrade = defaultdict(list)  # all the results directories, keyed by upgrades
    for item in fs.ls(sim_out_dir):
        m = re.search(r'up(\d+)', item)
        if not m:
            continue
        upgrade_id = int(m.group(1))
        for subitem in fs.ls(item):
            m = re.search(r'bldg(\d+)', subitem)
            if not m:
                continue
            results_by_upgrade[upgrade_id].append(subitem)
            all_dirs.append(subitem)

    # create the results.csv and results.parquet files
    for upgrade_id, sim_dir_list in results_by_upgrade.items():

        logger.info('Computing results for upgrade {} with {} simulations'.format(upgrade_id, len(sim_dir_list)))

        datapoint_output_jsons = db.from_sequence(sim_dir_list, partition_size=dask_bag_partition_size).\
            map("{}/run/data_point_out.json".format).\
            map(partial(read_data_point_out_json, fs, reporting_measures)).\
            filter(lambda x: x is not None)
        meta = pd.DataFrame(list(
            datapoint_output_jsons.filter(lambda x: 'SimulationOutputReport' in x.keys()).
            map(partial(flatten_datapoint_json, reporting_measures)).take(10)
        ))

        if meta.shape == (0, 0):
            meta = None

        data_point_out_df_d = datapoint_output_jsons.map(partial(flatten_datapoint_json, reporting_measures)).\
            to_dataframe(meta=meta).rename(columns=to_camelcase)

        out_osws = db.from_sequence(sim_dir_list, partition_size=dask_bag_partition_size).\
            map("{}/out.osw".format)

        out_osw_df_d = out_osws.map(partial(read_out_osw, fs)).filter(lambda x: x is not None).to_dataframe()

        data_point_out_df, out_osw_df = dask.compute(data_point_out_df_d, out_osw_df_d)

        results_df = out_osw_df.merge(data_point_out_df, how='left', on='building_unit_id')
        cols_to_remove = (
            'build_existing_model.weight',
            'simulation_output_report.weight',
            'build_existing_model.workflow_json',
            'simulation_output_report.upgrade_name'
        )
        for col in cols_to_remove:
            if col in results_df.columns:
                del results_df[col]
        for col in ('started_at', 'completed_at'):
            results_df[col] = results_df[col].map(lambda x: dt.datetime.strptime(x, '%Y%m%dT%H%M%SZ') if x is not None
                                                  else None)

        if upgrade_id > 0:
            cols_to_keep = list(
                filter(lambda x: not x.startswith('build_existing_model.'), results_df.columns)
            )
            results_df = results_df[cols_to_keep]

        # Add reference_scenario column to the dataframe
        # begin: find the name of the current upgrade
        upgrade_names = results_df['apply_upgrade.upgrade_name']
        valid_index = upgrade_names.first_valid_index()
        # some rows in results_upxx.csv might have blank entry for apply_upgrade.upgrade_name. Either because it is a
        # baseline simulation or because some of the simulation for an upgrade has failed.
        if valid_index is not None:  # only if there is a valid upgrade name for current upgrade
            current_upgrade_name = upgrade_names.iloc[valid_index]
        # end: find the name of the current upgrade
            for upgrade in config.get('upgrades', []):  # find the configuration for current upgrade
                if upgrade['upgrade_name'] == current_upgrade_name:
                    reference_scenario = upgrade.get('reference_scenario', None)  # and extract reference_scenario
                    if reference_scenario:
                        results_df['apply_upgrade.reference_scenario'] = reference_scenario
                        break

        # standardize the column orders
        first_few_cols = ['building_unit_id', 'started_at', 'completed_at', 'completed_status',
                          'apply_upgrade.applicable', 'apply_upgrade.upgrade_name', 'apply_upgrade.reference_scenario']

        build_existing_model_cols = sorted([col for col in results_df.columns if
                                            col.startswith('build_existing_model')])
        simulation_output_cols = sorted([col for col in results_df.columns if
                                         col.startswith('simulation_output_report')])
        sorted_cols = first_few_cols + build_existing_model_cols + simulation_output_cols

        for reporting_measure in reporting_measures:
            reporting_measure_cols = sorted([col for col in results_df.columns if
                                            col.startswith(to_camelcase(reporting_measure))])
            sorted_cols += reporting_measure_cols

        results_df = results_df.reindex(columns=sorted_cols, copy=False)

        # Save to CSV
        logger.debug('Saving to csv.gz')
        csv_filename = f"{results_csvs_dir}/results_up{upgrade_id:02d}.csv.gz"
        with fs.open(csv_filename, 'wb') as f:
            with gzip.open(f, 'wt', encoding='utf-8') as gf:
                results_df.to_csv(gf, index=False)

        # Save to parquet
        logger.debug('Saving to parquet')
        if upgrade_id == 0:
            results_parquet_dir = f"{parquet_dir}/baseline"
        else:
            results_parquet_dir = f"{parquet_dir}/upgrades/upgrade={upgrade_id}"
        if not fs.exists(results_parquet_dir):
            fs.makedirs(results_parquet_dir)
        write_dataframe_as_parquet(
            results_df,
            fs,
            f"{results_parquet_dir}/results_up{upgrade_id:02d}.parquet"
        )

        # combine and save the aggregated timeseries file
        if not skip_timeseries and aggregate_timeseries:
            logger.info(f"Combining timeseries files for {upgrade_id} in directory {sim_out_dir}")
            agg_parquet = db.from_sequence(sim_dir_list).fold(partial(add_timeseries, fs, sim_out_dir)).compute()
            agg_parquet.reset_index(inplace=True)

            agg_parquet_dir = f"{agg_ts_dir}/upgrade={upgrade_id}"
            if not fs.exists(agg_parquet_dir):
                fs.makedirs(agg_parquet_dir)

            write_dataframe_as_parquet(agg_parquet,
                                       fs,
                                       f"{agg_parquet_dir}/aggregated_ts_up{upgrade_id:02d}.parquet")

    if skip_timeseries:
        logger.info("Timeseries aggregation skipped.")
        return

    # Time series combine

    # Create directories in serial section to avoid race conditions
    for upgrade_id in results_by_upgrade.keys():
        folder_path = f"{ts_dir}/upgrade={upgrade_id}"
        if not fs.exists(folder_path):
            fs.makedirs(folder_path)

    # find the avg size of time_series parqeut files
    total_size = 0
    count = 0
    sample_size = 10 if len(all_dirs) >= 10 else len(all_dirs)
    for rnd_ts_index in random.sample(range(len(all_dirs)), sample_size):
        full_path = f"{all_dirs[rnd_ts_index]}/run/enduse_timeseries.parquet"
        try:
            with fs.open(full_path, 'rb') as f:
                pq = pd.read_parquet(f, engine='pyarrow')
        except FileNotFoundError:
            logger.warning(f" Time series file does not exist: {full_path}")
        else:
            total_size += sys.getsizeof(pq)
            count += 1

    if count == 0:
        logger.error('No valid timeseries file could be found.')
        return

    avg_parquet_size = total_size / count

    group_size = int(1.3*1024*1024*1024 / avg_parquet_size)
    if group_size < 1:
        group_size = 1

    logger.info(f"Each parquet file is {avg_parquet_size / (1024 * 1024) :.2f} MB in memory.")
    logger.info(f"Combining {group_size} of them together, so that the size in memory is around 1.5 GB")

    logger.info("Combining the parquets")
    partitions_to_write = []
    for upgrade_id, sim_dirs_in_upgrade in results_by_upgrade.items():
        partitions_to_write.extend([
            (upgrade_id, group_id, chunk)
            for group_id, chunk in enumerate(divide_chunks(sim_dirs_in_upgrade, group_size))
        ])
    t = time.time()
    dask.compute([dask.delayed(write_output)(fs, results_dir, x) for x in partitions_to_write])
    diff = time.time() - t
    logger.info(f"Took {diff:.2f} seconds")


def upload_results(aws_conf, output_dir, results_dir):
    logger.info("Uploading the parquet files to s3")

    output_folder_name = Path(output_dir).name
    parquet_dir = Path(results_dir).joinpath('parquet')

    if not parquet_dir.is_dir():
        logger.error(f"{parquet_dir} does not exist. Please make sure postprocessing has been done.")
        raise FileNotFoundError(parquet_dir)

    all_files = []
    for files in parquet_dir.rglob('*.parquet'):
        all_files.append(files.relative_to(parquet_dir))

    s3_prefix = aws_conf.get('s3', {}).get('prefix', None)
    s3_bucket = aws_conf.get('s3', {}).get('bucket', None)
    if not (s3_prefix and s3_bucket):
        logger.error("YAML file missing postprocessing:aws:s3:prefix and/or bucket entry.")
        return
    s3_prefix_output = s3_prefix + '/' + output_folder_name + '/'

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)
    n_existing_files = len(list(bucket.objects.filter(Prefix=s3_prefix_output)))
    if n_existing_files > 0:
        logger.error(f"There are already {n_existing_files} files in the s3 folder {s3_bucket}/{s3_prefix_output}.")
        raise FileExistsError(f"s3://{s3_bucket}/{s3_prefix_output}")

    def upload_file(filepath):
        full_path = parquet_dir.joinpath(filepath)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(s3_bucket)
        s3key = Path(s3_prefix_output).joinpath(filepath).as_posix()
        bucket.upload_file(str(full_path), str(s3key))

    dask.compute(map(dask.delayed(upload_file), all_files))
    logger.info(f"Upload to S3 completed. The files are uploaded to: {s3_bucket}/{s3_prefix_output}")
    return s3_bucket, s3_prefix_output


def create_athena_tables(aws_conf, tbl_prefix, s3_bucket, s3_prefix):
    logger.info("Creating Athena tables using glue crawler")

    region_name = aws_conf.get('region_name', 'us-west-2')
    db_name = aws_conf.get('athena', {}).get('database_name', None)
    role = aws_conf.get('athena', {}).get('glue_service_role', 'service-role/AWSGlueServiceRole-default')
    max_crawling_time = aws_conf.get('athena', {}).get('max_crawling_time', 600)
    assert db_name, "athena:database_name not supplied"

    glueClient = boto3.client('glue', region_name=region_name)
    crawlTarget = {
        'S3Targets': [{
            'Path': f's3://{s3_bucket}/{s3_prefix}',
            'Exclusions': []
        }]
    }
    crawler_name = db_name + '_' + tbl_prefix
    tbl_prefix = tbl_prefix + '_'

    def create_crawler():
        glueClient.create_crawler(Name=crawler_name,
                                  Role=role,
                                  Targets=crawlTarget,
                                  DatabaseName=db_name,
                                  TablePrefix=tbl_prefix)

    try:
        create_crawler()
    except glueClient.exceptions.AlreadyExistsException:
        logger.info(f"Deleting existing crawler: {crawler_name}. And creating new one.")
        glueClient.delete_crawler(Name=crawler_name)
        time.sleep(1)  # A small delay after deleting is required to prevent AlreadyExistsException again
        create_crawler()

    try:
        existing_tables = [x['Name'] for x in glueClient.get_tables(DatabaseName=db_name)['TableList']]
    except glueClient.exceptions.EntityNotFoundException:
        existing_tables = []

    to_be_deleted_tables = [x for x in existing_tables if x.startswith(tbl_prefix)]
    if to_be_deleted_tables:
        logger.info(f"Deleting existing tables in db {db_name}: {to_be_deleted_tables}. And creating new ones.")
        glueClient.batch_delete_table(DatabaseName=db_name, TablesToDelete=to_be_deleted_tables)

    glueClient.start_crawler(Name=crawler_name)
    logger.info("Crawler started")
    is_crawler_running = True
    t = time.time()
    while time.time() - t < (3 * max_crawling_time):
        crawler_state = glueClient.get_crawler(Name=crawler_name)['Crawler']['State']
        metrics = glueClient.get_crawler_metrics(CrawlerNameList=[crawler_name])['CrawlerMetricsList'][0]
        if is_crawler_running and crawler_state != 'RUNNING':
            is_crawler_running = False
            logger.info(f"Crawler has completed running. It is {crawler_state}.")
            logger.info(f"TablesCreated: {metrics['TablesCreated']} "
                        f"TablesUpdated: {metrics['TablesUpdated']} "
                        f"TablesDeleted: {metrics['TablesDeleted']} ")
        if crawler_state == 'READY':
            logger.info("Crawler stopped. Deleting it now.")
            glueClient.delete_crawler(Name=crawler_name)
            break
        elif time.time() - t > max_crawling_time:
            logger.info("Crawler is taking too long. Aborting ...")
            logger.info(f"TablesCreated: {metrics['TablesCreated']} "
                        f"TablesUpdated: {metrics['TablesUpdated']} "
                        f"TablesDeleted: {metrics['TablesDeleted']} ")
            glueClient.stop_crawler(Name=crawler_name)
        elif time.time() - t > 2 * max_crawling_time:
            logger.warning(f"Crawler could not be stopped and deleted. Please delete the crawler {crawler_name} "
                           f"manually from the AWS console")
            break
        time.sleep(30)
