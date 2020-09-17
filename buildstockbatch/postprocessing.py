# -*- coding: utf-8 -*-

"""
buildstockbatch.postprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A module containing utility functions for postprocessing

:author: Noel Merket, Rajendra Adhikari
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import boto3
import dask.bag as db
import dask.dataframe as dd
import dask
import datetime as dt
from fsspec.implementations.local import LocalFileSystem
from functools import partial
import gzip
import itertools
import json
import logging
import math
import numpy as np
import pandas as pd
from pathlib import Path
import pyarrow as pa
from pyarrow import parquet
import random
import re
from s3fs import S3FileSystem
import time

logger = logging.getLogger(__name__)

MAX_PARQUET_MEMORY = 1e9  # maximum size of the parquet file in memory when combining multiple parquets


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
    # TODO @nmerket @rajeee is there a way to not apply this to Commercial jobs? It doesn't hurt, but it is weird for us
    units = int(new_d.get(f'{col1}.units_represented', 1))
    new_d[f'{col1}.units_represented'] = units
    col2 = 'SimulationOutputReport'
    for k, v in d.get(col2, {}).items():
        new_d[f'{col2}.{k}'] = v

    # additional reporting measures
    for col in reporting_measures:
        for k, v in d.get(col, {}).items():
            new_d[f'{col}.{k}'] = v

    new_d['building_id'] = new_d['BuildExistingModel.building_unit_id']
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
                out_d['building_id'] = step['arguments']['building_unit_id']
        return out_d


def read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, building_id):
    """Read the simulation outputs and return as a dict

    :param fs: filesystem to read from
    :type fs: fsspec filesystem
    :param reporting_measures: a list of reporting measure to pull results from
    :type reporting_measures: list[str]
    :param sim_dir: path to simulation output directory
    :type sim_dir: str
    :param upgrade_id: id for upgrade, 0 for baseline, 1, 2...
    :type upgrade_id: int
    :param building_id: building id
    :type building_id: int
    :return: dpout [dict]
    """

    dpout = read_data_point_out_json(
        fs, reporting_measures, f'{sim_dir}/run/data_point_out.json'
    )
    if dpout is None:
        dpout = {}
    else:
        dpout = flatten_datapoint_json(reporting_measures, dpout)
    out_osw = read_out_osw(fs, f'{sim_dir}/out.osw')
    if out_osw:
        dpout.update(out_osw)
    dpout['upgrade'] = upgrade_id
    dpout['building_id'] = building_id
    return dpout


def write_dataframe_as_parquet(df, fs, filename):
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(filename, 'wb') as f:
        parquet.write_table(tbl, f, flavor='spark')


def clean_up_results_df(df, cfg, keep_upgrade_id=False):
    results_df = df.copy()
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
        if col in results_df.columns:
            results_df[col] = results_df[col].map(
                lambda x: dt.datetime.strptime(x, '%Y%m%dT%H%M%SZ') if isinstance(x, str) else x
            )
    reference_scenarios = dict([(i, x.get('reference_scenario')) for i, x in enumerate(cfg.get('upgrades', []), 1)])
    results_df['apply_upgrade.reference_scenario'] = \
        results_df['upgrade'].map(reference_scenarios).fillna('').astype(str)

    # standardize the column orders
    first_few_cols = [
        'building_id',
        'started_at',
        'completed_at',
        'completed_status',
        'apply_upgrade.applicable',
        'apply_upgrade.upgrade_name',
        'apply_upgrade.reference_scenario'
    ]
    if keep_upgrade_id:
        first_few_cols.insert(1, 'upgrade')
    if 'job_id' in results_df.columns:
        first_few_cols.insert(2, 'job_id')

    build_existing_model_cols = sorted([col for col in results_df.columns if col.startswith('build_existing_model')])
    simulation_output_cols = sorted([col for col in results_df.columns if col.startswith('simulation_output_report')])
    sorted_cols = first_few_cols + build_existing_model_cols + simulation_output_cols

    for reporting_measure in cfg.get('reporting_measures', []):
        reporting_measure_cols = sorted([col for col in results_df.columns if
                                        col.startswith(to_camelcase(reporting_measure))])
        sorted_cols += reporting_measure_cols

    results_df = results_df.reindex(columns=sorted_cols, copy=False)

    # for col in results_df.columns:
    #     if col.startswith('simulation_output_report.') and not col == 'simulation_output_report.applicable':
    #         results_df[col] = pd.to_numeric(results_df[col], errors='coerce')

    return results_df


def get_cols(fs, filename):
    with fs.open(filename, 'rb') as f:
        schema = parquet.read_schema(f)
    return schema.names


def read_results_json(fs, filename):
    with fs.open(filename, 'rb') as f1:
        with gzip.open(f1, 'rt', encoding='utf-8') as f2:
            dpouts = json.load(f2)
    return dpouts


def read_enduse_timeseries_parquet(fs, filename, all_cols):
    with fs.open(filename, 'rb') as f:
        df = pd.read_parquet(f, engine='pyarrow')
    building_id = int(re.search(r'bldg(\d+).parquet', filename).group(1))
    df['building_id'] = building_id
    for col in set(all_cols).difference(df.columns.values):
        df[col] = np.nan
    return df[all_cols]


def read_and_concat_enduse_timeseries_parquet(fs, filenames, all_cols):
    return pd.concat(read_enduse_timeseries_parquet(fs, filename, all_cols) for filename in filenames)


def combine_results(fs, results_dir, cfg, do_timeseries=True):
    """Combine the results of the batch simulations.

    :param fs: fsspec filesystem (currently supports local and s3)
    :type fs: fsspec filesystem
    :param results_dir: directory where results are stored and written
    :type results_dir: str
    :param cfg: project configuration (contents of yaml file)
    :type cfg: dict
    :param do_timeseries: process timeseries results, defaults to True
    :type do_timeseries: bool, optional
    """
    sim_output_dir = f'{results_dir}/simulation_output'
    ts_in_dir = f'{sim_output_dir}/timeseries'
    results_csvs_dir = f'{results_dir}/results_csvs'
    parquet_dir = f'{results_dir}/parquet'
    ts_dir = f'{results_dir}/parquet/timeseries'
    dirs = [results_csvs_dir, parquet_dir]
    if do_timeseries:
        dirs.append(ts_dir)

    # create the postprocessing results directories
    for dr in dirs:
        fs.makedirs(dr)

    # Results "CSV"
    results_job_json_glob = f'{sim_output_dir}/results_job*.json.gz'
    results_jsons = fs.glob(results_job_json_glob)
    results_json_job_ids = [int(re.search(r'results_job(\d+)\.json\.gz', x).group(1)) for x in results_jsons]
    dpouts_by_job = dask.compute([dask.delayed(read_results_json)(fs, x) for x in results_jsons])[0]
    for job_id, dpouts_for_this_job in zip(results_json_job_ids, dpouts_by_job):
        for dpout in dpouts_for_this_job:
            dpout['job_id'] = job_id
    dpouts = itertools.chain.from_iterable(dpouts_by_job)
    results_df = pd.DataFrame(dpouts).rename(columns=to_camelcase)

    del dpouts

    if results_df.empty:
        raise ValueError("No simulation results found to post-process")

    results_df = clean_up_results_df(results_df, cfg, keep_upgrade_id=True)

    if do_timeseries:

        # Look at all the parquet files to see what columns are in all of them.
        ts_filenames = fs.glob(f'{ts_in_dir}/up*/bldg*.parquet')
        all_ts_cols = db.from_sequence(ts_filenames, partition_size=100).map(partial(get_cols, fs)).\
            fold(lambda x, y: set(x).union(y)).compute()

        # Sort the columns
        all_ts_cols_sorted = ['building_id'] + sorted(x for x in all_ts_cols if x.startswith('time'))
        all_ts_cols.difference_update(all_ts_cols_sorted)
        all_ts_cols_sorted.extend(sorted(x for x in all_ts_cols if not x.endswith(']')))
        all_ts_cols.difference_update(all_ts_cols_sorted)
        all_ts_cols_sorted.extend(sorted(all_ts_cols))

    for upgrade_id, df in results_df.groupby('upgrade'):
        if upgrade_id > 0:
            # Remove building characteristics for upgrade scenarios.
            cols_to_keep = list(
                filter(lambda x: not x.startswith('build_existing_model.'), results_df.columns)
            )
            df = df[cols_to_keep]
        df = df.copy()
        del df['upgrade']
        df.set_index('building_id', inplace=True)
        df.sort_index(inplace=True)

        # Write CSV
        csv_filename = f"{results_csvs_dir}/results_up{upgrade_id:02d}.csv.gz"
        logger.info(f'Writing {csv_filename}')
        with fs.open(csv_filename, 'wb') as f:
            with gzip.open(f, 'wt', encoding='utf-8') as gf:
                df.to_csv(gf, index=True, line_terminator='\n')

        # Write Parquet
        if upgrade_id == 0:
            results_parquet_dir = f"{parquet_dir}/baseline"
        else:
            results_parquet_dir = f"{parquet_dir}/upgrades/upgrade={upgrade_id}"
        if not fs.exists(results_parquet_dir):
            fs.makedirs(results_parquet_dir)
        write_dataframe_as_parquet(
            df.reset_index(),
            fs,
            f"{results_parquet_dir}/results_up{upgrade_id:02d}.parquet"
        )

        if do_timeseries:

            # Get the names of the timseries file for each simulation in this upgrade
            ts_filenames = fs.glob(f'{ts_in_dir}/up{upgrade_id:02d}/bldg*.parquet')

            # Calculate the mean and estimate the total memory usage
            read_ts_parquet = partial(read_enduse_timeseries_parquet, fs, all_cols=all_ts_cols_sorted)
            get_ts_mem_usage_d = dask.delayed(lambda x: read_ts_parquet(x).memory_usage(deep=True).sum())
            sample_size = min(len(ts_filenames), 36 * 3)
            mean_mem = np.mean(dask.compute(map(get_ts_mem_usage_d, random.sample(ts_filenames, sample_size)))[0])
            total_mem = mean_mem * len(ts_filenames)

            # Determine how many files should be in each partition and group the files
            npartitions = math.ceil(total_mem / MAX_PARQUET_MEMORY)  # 1 GB per partition
            npartitions = min(len(ts_filenames), npartitions)  # cannot have less than one file per partition
            ts_files_in_each_partition = np.array_split(ts_filenames, npartitions)

            # Read the timeseries into a dask dataframe
            read_and_concat_ts_pq_d = dask.delayed(
                partial(read_and_concat_enduse_timeseries_parquet, fs, all_cols=all_ts_cols_sorted)
            )
            ts_df = dd.from_delayed(map(read_and_concat_ts_pq_d, ts_files_in_each_partition))
            ts_df = ts_df.set_index('building_id', sorted=True)

            # Write out new dask timeseries dataframe.
            if isinstance(fs, LocalFileSystem):
                ts_out_loc = f"{ts_dir}/upgrade={upgrade_id}"
            else:
                assert isinstance(fs, S3FileSystem)
                ts_out_loc = f"s3://{ts_dir}/upgrade={upgrade_id}"
            logger.info(f'Writing {ts_out_loc}')
            ts_df.to_parquet(
                ts_out_loc,
                engine='pyarrow',
                flavor='spark'
            )


def remove_intermediate_files(fs, results_dir):
    # Remove aggregated files to save space
    sim_output_dir = f'{results_dir}/simulation_output'
    ts_in_dir = f'{sim_output_dir}/timeseries'
    results_job_json_glob = f'{sim_output_dir}/results_job*.json.gz'
    logger.info('Removing temporary files')
    fs.rm(ts_in_dir, recursive=True)
    for filename in fs.glob(results_job_json_glob):
        fs.rm(filename)


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
