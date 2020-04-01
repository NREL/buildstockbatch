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
from collections import defaultdict
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
import sys
import time

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

    new_d['building_id'] = new_d['BuildExistingModel.building_id']
    del new_d['BuildExistingModel.building_id']

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
                out_d['building_id'] = step['arguments']['building_id']
        return out_d


def read_simulation_outputs(fs, reporting_measures, sim_dir):
    """Reat the simulation outputs and return as a dict

    :param fs: filesystem to read from
    :type fs: fsspec filesystem
    :param reporting_measures: a list of reporting measure to pull results from
    :type reporting_measures: list[str]
    :param sim_dir: path to simulation output directory
    :type sim_dir: str
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
    return dpout


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


def write_ts_output(fs, ts_dir, args):
    upgrade_id, group_id, folders = args

    folder_path = f"{ts_dir}/upgrade={upgrade_id}"
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

        building_id_match = re.search(r'bldg(\d+)', folder)
        assert building_id_match, f"The building results folder format should be: ~bldg(\\d+). Got: {folder} "
        building_id = int(building_id_match.group(1))
        new_pq['building_id'] = building_id
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
        results_df[col] = results_df[col].map(lambda x: dt.datetime.strptime(x, '%Y%m%dT%H%M%SZ') if x is not None
                                              else None)
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


def get_results_by_upgrade(fs, sim_out_dir):
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
    return results_by_upgrade


def timeseries_combine(fs, sim_out_dir, ts_dir):
    """
    Combine the enduse_timeseries.parquet files into fewer, larger files and organize by upgrade

    :param fs: fsspec filesystem
    :param sim_out_dir: directory containing simulation outputs to aggregate
    :param ts_dir: directory to output aggregated timeseries data
    """

    # Delete and create anew the ts_dir
    if fs.exists(ts_dir):
        fs.rm(ts_dir, recursive=True)
    fs.makedirs(ts_dir)

    results_by_upgrade = get_results_by_upgrade(fs, sim_out_dir)
    # get the list of all the building simulation results directories
    all_dirs = list(itertools.chain.from_iterable(results_by_upgrade.values()))

    # Create directories in serial section to avoid race conditions
    for upgrade_id in results_by_upgrade.keys():
        folder_path = f"{ts_dir}/upgrade={upgrade_id}"
        if not fs.exists(folder_path):
            fs.makedirs(folder_path)

    # find the avg size of time_series parquet files
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
    dask.compute([dask.delayed(write_ts_output)(fs, ts_dir, x) for x in partitions_to_write])
    diff = time.time() - t
    logger.info(f"Combining the parquets took {diff:.2f} seconds")


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

    results_by_upgrade = get_results_by_upgrade(fs, sim_out_dir)

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

        results_df = out_osw_df.merge(data_point_out_df, how='left', on='building_id')
        results_df['upgrade'] = upgrade_id

        results_df = clean_up_results_df(results_df, config)

        if upgrade_id > 0:
            cols_to_keep = list(
                filter(lambda x: not x.startswith('build_existing_model.'), results_df.columns)
            )
            results_df = results_df[cols_to_keep]

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

    timeseries_combine(fs, sim_out_dir, ts_dir)


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


def combine_results2(fs, results_dir, cfg, do_timeseries=True):
    sim_output_dir = f'{results_dir}/simulation_output'
    ts_in_dir = f'{sim_output_dir}/timeseries'
    results_csvs_dir = f'{results_dir}/results_csvs'
    parquet_dir = f'{results_dir}/parquet'
    ts_dir = f'{results_dir}/parquet/timeseries'
    dirs = [results_csvs_dir, parquet_dir]
    if do_timeseries:
        dirs.append(ts_dir)

    # clear and create the postprocessing results directories
    for dr in dirs:
        if fs.exists(dr):
            fs.rm(dr, recursive=True)
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
                df.to_csv(gf, index=True)

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
            npartitions = math.ceil(total_mem / 300e6)  # 300 MB per partition
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

    # Remove aggregated files to save space
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
