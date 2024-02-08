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
import botocore.exceptions
import dask.bag as db
from dask.distributed import performance_report
import dask
import dask.dataframe as dd
from dask.dataframe.io.parquet import create_metadata_file
from fsspec.implementations.local import LocalFileSystem
from functools import partial
import gzip
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
import tempfile
import time

logger = logging.getLogger(__name__)

MAX_PARQUET_MEMORY = 1000  # maximum size (MB) of the parquet file in memory when combining multiple parquets


def read_data_point_out_json(fs, reporting_measures, filename):
    try:
        with fs.open(filename, "r") as f:
            d = json.load(f)
        if not d:
            return None
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    else:
        sim_out_report = "SimulationOutputReport"
        if "ReportSimulationOutput" in d:
            sim_out_report = "ReportSimulationOutput"

        if sim_out_report not in d:
            d[sim_out_report] = {"applicable": False}
        for reporting_measure in reporting_measures:
            if reporting_measure not in d:
                d[reporting_measure] = {"applicable": False}
        return d


def to_camelcase(x):
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", x)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def flatten_datapoint_json(reporting_measures, d):
    new_d = {}
    cols_to_keep = {"ApplyUpgrade": ["upgrade_name", "applicable"]}
    for k1, k2s in cols_to_keep.items():
        for k2 in k2s:
            new_d[f"{k1}.{k2}"] = d.get(k1, {}).get(k2)

    # copy over all the key and values from BuildExistingModel
    col1 = "BuildExistingModel"
    for k, v in d.get(col1, {}).items():
        new_d[f"{col1}.{k}"] = v

    # if there is no units_represented key, default to 1
    # TODO @nmerket @rajeee is there a way to not apply this to Commercial jobs? It doesn't hurt, but it is weird for us
    units = int(new_d.get(f"{col1}.units_represented", 1))
    new_d[f"{col1}.units_represented"] = units
    sim_out_report = "SimulationOutputReport"
    if "ReportSimulationOutput" in d:
        sim_out_report = "ReportSimulationOutput"
    col2 = sim_out_report
    for k, v in d.get(col2, {}).items():
        new_d[f"{col2}.{k}"] = v

    # additional reporting measures
    if sim_out_report == "ReportSimulationOutput":
        reporting_measures += ["ReportUtilityBills"]
        reporting_measures += ["UpgradeCosts"]
    for col in reporting_measures:
        for k, v in d.get(col, {}).items():
            new_d[f"{col}.{k}"] = v

    new_d["building_id"] = new_d["BuildExistingModel.building_id"]
    del new_d["BuildExistingModel.building_id"]

    return new_d


def read_out_osw(fs, filename):
    try:
        with fs.open(filename, "r") as f:
            d = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    else:
        out_d = {}
        keys_to_copy = ["started_at", "completed_at", "completed_status"]
        for key in keys_to_copy:
            out_d[key] = d.get(key, None)
        for step in d.get("steps", []):
            if step["measure_dir_name"] == "BuildExistingModel":
                out_d["building_id"] = step["arguments"]["building_id"]
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

    dpout = read_data_point_out_json(fs, reporting_measures, f"{sim_dir}/run/data_point_out.json")
    if dpout is None:
        dpout = {}
    else:
        dpout = flatten_datapoint_json(reporting_measures, dpout)
    out_osw = read_out_osw(fs, f"{sim_dir}/out.osw")
    if out_osw:
        dpout.update(out_osw)
    dpout["upgrade"] = upgrade_id
    dpout["building_id"] = building_id
    return dpout


def write_dataframe_as_parquet(df, fs, filename, schema=None):
    tbl = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    with fs.open(filename, "wb") as f:
        parquet.write_table(tbl, f)


def clean_up_results_df(df, cfg, keep_upgrade_id=False):
    results_df = df.copy()
    cols_to_remove = (
        "build_existing_model.weight",
        "simulation_output_report.weight",
        "build_existing_model.workflow_json",
        "simulation_output_report.upgrade_name",
    )
    for col in cols_to_remove:
        if col in results_df.columns:
            del results_df[col]
    for col in ("started_at", "completed_at"):
        if col in results_df.columns:
            results_df[col] = pd.to_datetime(results_df[col], format="%Y%m%dT%H%M%SZ").astype(
                pd.ArrowDtype(pa.timestamp("s"))
            )
    reference_scenarios = dict([(i, x.get("reference_scenario")) for i, x in enumerate(cfg.get("upgrades", []), 1)])
    results_df["apply_upgrade.reference_scenario"] = (
        results_df["upgrade"].map(reference_scenarios).fillna("").astype(str)
    )

    # standardize the column orders
    first_few_cols = [
        "building_id",
        "started_at",
        "completed_at",
        "completed_status",
        "apply_upgrade.applicable",
        "apply_upgrade.upgrade_name",
        "apply_upgrade.reference_scenario",
    ]
    if keep_upgrade_id:
        first_few_cols.insert(1, "upgrade")
    if "job_id" in results_df.columns:
        first_few_cols.insert(2, "job_id")

    build_existing_model_cols = sorted([col for col in results_df.columns if col.startswith("build_existing_model")])
    sim_output_report_cols = sorted([col for col in results_df.columns if col.startswith("simulation_output_report")])
    report_sim_output_cols = sorted([col for col in results_df.columns if col.startswith("report_simulation_output")])
    upgrade_costs_cols = sorted([col for col in results_df.columns if col.startswith("upgrade_costs")])
    sorted_cols = (
        first_few_cols
        + build_existing_model_cols
        + sim_output_report_cols
        + report_sim_output_cols
        + upgrade_costs_cols
    )

    remaining_cols = sorted(set(results_df.columns.values).difference(sorted_cols))
    sorted_cols += remaining_cols

    results_df = results_df.reindex(columns=sorted_cols, copy=False)
    results_df = results_df.convert_dtypes(dtype_backend="pyarrow")

    return results_df


def get_cols(fs, filepath):
    with fs.open(filepath, "rb") as f:
        schema = parquet.read_schema(f)
    return set(schema.names)


def read_results_json(fs, filename, all_cols=None):
    with fs.open(filename, "rb") as f1:
        with gzip.open(f1, "rt", encoding="utf-8") as f2:
            dpouts = json.load(f2)
    df = pd.DataFrame(dpouts)
    df["job_id"] = int(re.search(r"results_job(\d+)\.json\.gz", filename).group(1))
    if all_cols is not None:
        for missing_col in set(all_cols).difference(df.columns.values):
            df[missing_col] = None
    # Sorting is needed to ensure all dfs have same column order. Dask will fail otherwise.
    df = df.reindex(sorted(df.columns), axis=1).convert_dtypes(dtype_backend="pyarrow")
    return df


def get_schema_dict(fs, filename):
    df = read_results_json(fs, filename)
    df = df.replace("", np.nan)  # required to make pa correctly infer the dtypes
    sch = pa.Schema.from_pandas(df)
    sch_dict = {name: type for name, type in zip(sch.names, sch.types)}
    return sch_dict


def merge_schema_dicts(dict1, dict2):
    new_dict = dict(dict1)
    for col, dtype2 in dict2.items():
        dtype1 = new_dict.get(col)
        if col not in new_dict or dtype1 == pa.null():
            new_dict[col] = dtype2
    return new_dict


def read_enduse_timeseries_parquet(fs, all_cols, src_path, bldg_id):
    src_filename = f"{src_path}/bldg{bldg_id:07}.parquet"
    with fs.open(src_filename, "rb") as f:
        df = pd.read_parquet(f, engine="pyarrow")
    df["building_id"] = bldg_id
    for col in set(all_cols).difference(df.columns.values):
        df[col] = np.nan
    df = df[all_cols]
    df.set_index("building_id", inplace=True)
    return df


def concat_and_normalize(fs, all_cols, src_path, dst_path, partition_columns, indx, bldg_ids, partition_vals):
    dfs = []
    for bldg_id in sorted(bldg_ids):
        df = read_enduse_timeseries_parquet(fs, all_cols, src_path, bldg_id)
        dfs.append(df)
    df = pd.concat(dfs)
    del dfs

    dst_filepath = dst_path
    for col, val in zip(partition_columns, partition_vals):
        folder_name = f"{col}={val}"
        dst_filepath = f"{dst_filepath}/{folder_name}"

    fs.makedirs(dst_filepath, exist_ok=True)
    dst_filename = f"{dst_filepath}/group{indx}.parquet"
    with fs.open(dst_filename, "wb") as f:
        df.to_parquet(f, index=True)
    return len(bldg_ids)


def get_null_cols(df):
    sch = pa.Schema.from_pandas(df)
    null_cols = []
    for col, dtype in zip(sch.names, sch.types):
        if dtype == pa.null():
            null_cols.append(col)
    return null_cols


def correct_schema(cur_schema_dict, df):
    sch = pa.Schema.from_pandas(df)
    sch_dict = {name: type for name, type in zip(sch.names, sch.types)}
    unresolved = []
    for col, dtype in sch_dict.items():
        if dtype == pa.null():
            if col in cur_schema_dict:
                indx = sch.get_field_index(col)
                sch = sch.set(indx, pa.field(col, cur_schema_dict.get(col)))
            else:
                unresolved.append(col)
    return sch, unresolved


def split_into_groups(total_size, max_group_size):
    """
    Splits an integer into sum of integers (returned as an array) each not exceeding max_group_size
    e.g. split_into_groups(10, 3) = [3, 3, 2, 2]
    """
    if total_size == 0:
        return []
    total_groups = math.ceil(total_size / max_group_size)
    min_elements_per_group = math.floor(total_size / total_groups)
    split_array = [min_elements_per_group] * total_groups
    remainder = total_size - min_elements_per_group * total_groups
    assert 0 <= remainder < len(split_array)
    for i in range(remainder):
        split_array[i] += 1
    return split_array


def get_partitioned_bldg_groups(partition_df, partition_columns, files_per_partition):
    """
    Returns intelligent grouping of building_ids by partition columns.
    1. Group the building_ids by partition columns. For each group, say (CO, Jefferson), we have a list of building
       ids. The total number of such groups is ngroups
    2. Concatenate those list to get bldg_id_list, which will have all the bldg_ids but ordered such that that
       buildings belonging to the same group are close together.
    3. Split the list of building in each group in 1 to multiple subgroups so that total number of buildings
       in each subgroup is less than or equal to files_per_partition. This will give the bldg_id_groups (list of
       list) used to read the dataframe. The buildings within the inner list will be concatenated.
       len(bldg_id_groups) is equal to number of such concatenation, and eventually, number of output parquet files.
    """
    total_building = len(partition_df)
    if partition_columns:
        bldg_id_list_df = partition_df.reset_index().groupby(partition_columns)["building_id"].apply(list)
        ngroups = len(bldg_id_list_df)
        bldg_id_list = bldg_id_list_df.sum()
        nfiles_in_each_group = [nfiles for nfiles in bldg_id_list_df.map(lambda x: len(x))]
        files_groups = [split_into_groups(n, files_per_partition) for n in nfiles_in_each_group]
        flat_groups = [n for group in files_groups for n in group]  # flatten list of list into a list (maintain order)
    else:
        # no partitioning by a column. Just put buildings into groups of files_per_partition
        ngroups = 1
        bldg_id_list = list(partition_df.index)
        flat_groups = split_into_groups(total_building, files_per_partition)

    cum_files_count = np.cumsum(flat_groups)
    assert cum_files_count[-1] == total_building
    cur_index = 0
    bldg_id_groups = []
    for indx in cum_files_count:
        bldg_id_groups.append(bldg_id_list[cur_index:indx])
        cur_index = indx

    return bldg_id_groups, bldg_id_list, ngroups


def get_upgrade_list(cfg):
    upgrade_start = 1 if cfg["baseline"].get("skip_sims", False) else 0
    upgrade_end = len(cfg.get("upgrades", [])) + 1
    return list(range(upgrade_start, upgrade_end))


def write_metadata_files(fs, parquet_root_dir, partition_columns):
    df = dd.read_parquet(parquet_root_dir)
    sch = pa.Schema.from_pandas(df._meta_nonempty)
    parquet.write_metadata(sch, f"{parquet_root_dir}/_common_metadata")
    logger.info(f"Written _common_metadata to {parquet_root_dir}")

    if partition_columns:
        partition_glob = "/".join([f"{c}*" for c in partition_columns])
        glob_str = f"{parquet_root_dir}/up*/{partition_glob}/*.parquet"
    else:
        glob_str = f"{parquet_root_dir}/up*/*.parquet"

    logger.info(f"Gathering all the parquet files in {glob_str}")
    concat_files = fs.glob(glob_str)
    logger.info(f"Gathered {len(concat_files)} files. Now writing _metadata")
    parquet_root_dir = Path(parquet_root_dir).as_posix()
    create_metadata_file(concat_files, root_dir=parquet_root_dir, engine="pyarrow", fs=fs)
    logger.info(f"_metadata file written to {parquet_root_dir}")


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
    sim_output_dir = f"{results_dir}/simulation_output"
    ts_in_dir = f"{sim_output_dir}/timeseries"
    results_csvs_dir = f"{results_dir}/results_csvs"
    parquet_dir = f"{results_dir}/parquet"
    ts_dir = f"{results_dir}/parquet/timeseries"
    dirs = [results_csvs_dir, parquet_dir]
    if do_timeseries:
        dirs.append(ts_dir)

    # create the postprocessing results directories
    for dr in dirs:
        fs.makedirs(dr)

    # Results "CSV"
    results_json_files = fs.glob(f"{sim_output_dir}/results_job*.json.gz")
    if not results_json_files:
        raise ValueError("No simulation results found to post-process.")

    logger.info("Collecting all the columns and datatypes in results_job*.json.gz parquet files.")
    all_schema_dict = (
        db.from_sequence(results_json_files)
        .map(partial(get_schema_dict, fs))
        .fold(lambda x, y: merge_schema_dicts(x, y))
        .compute()
    )
    logger.info(f"Got {len(all_schema_dict)} columns")
    all_results_cols = list(all_schema_dict.keys())
    all_schema_dict = {to_camelcase(key): value for key, value in all_schema_dict.items()}
    logger.info(f"Got this schema: {all_schema_dict}\n")
    delayed_results_dfs = [
        dask.delayed(partial(read_results_json, fs, all_cols=all_results_cols))(x) for x in results_json_files
    ]
    results_df = dd.from_delayed(delayed_results_dfs, verify_meta=False)

    if do_timeseries:
        # Look at all the parquet files to see what columns are in all of them.
        logger.info("Collecting all the columns in timeseries parquet files.")
        do_timeseries = False
        all_ts_cols = set()
        for upgrade_folder in fs.glob(f"{ts_in_dir}/up*"):
            ts_filenames = fs.ls(upgrade_folder)
            if ts_filenames:
                do_timeseries = True
                logger.info(f"Found {len(ts_filenames)} files for upgrade {Path(upgrade_folder).name}.")
                files_bag = db.from_sequence(ts_filenames, partition_size=100)
                all_ts_cols |= files_bag.map(partial(get_cols, fs)).fold(lambda x, y: x.union(y)).compute()
                logger.info("Collected all the columns")
            else:
                logger.info(f"There are no timeseries files for upgrade {Path(upgrade_folder).name}.")

        # Sort the columns
        all_ts_cols_sorted = ["building_id"] + sorted(x for x in all_ts_cols if x.startswith("time"))
        all_ts_cols.difference_update(all_ts_cols_sorted)
        all_ts_cols_sorted.extend(sorted(x for x in all_ts_cols if not x.endswith("]")))
        all_ts_cols.difference_update(all_ts_cols_sorted)
        all_ts_cols_sorted.extend(sorted(all_ts_cols))
        logger.info(f"Got {len(all_ts_cols_sorted)} columns in total")
        logger.info(f"The columns are: {all_ts_cols_sorted}")
    else:
        logger.warning("There are no timeseries files for any upgrades.")

    results_df_groups = results_df.groupby("upgrade")
    upgrade_list = get_upgrade_list(cfg)
    partition_columns = cfg.get("postprocessing", {}).get("partition_columns", [])
    partition_columns = [c.lower() for c in partition_columns]
    df_partition_columns = [f"build_existing_model.{c}" for c in partition_columns]
    missing_cols = set(df_partition_columns) - set(all_schema_dict.keys())
    if missing_cols:
        raise ValueError(f"The following partitioning columns are not found in results.json: {missing_cols}")
    if partition_columns:
        logger.info(f"The timeseries files will be partitioned by {partition_columns}.")

    logger.info(f"Will postprocess the following upgrades {upgrade_list}")
    for upgrade_id in upgrade_list:
        logger.info(f"Processing upgrade {upgrade_id}. ")
        df = dask.compute(results_df_groups.get_group(upgrade_id))[0]
        logger.info(f"Obtained results_df for {upgrade_id} with {len(df)} datapoints. ")
        df.rename(columns=to_camelcase, inplace=True)
        df = clean_up_results_df(df, cfg, keep_upgrade_id=True)
        del df["upgrade"]
        df.set_index("building_id", inplace=True)
        df.sort_index(inplace=True)
        schema = None
        partition_df = df[df_partition_columns].copy()
        partition_df.rename(
            columns={df_c: c for df_c, c in zip(df_partition_columns, partition_columns)},
            inplace=True,
        )
        if upgrade_id > 0:
            # Remove building characteristics for upgrade scenarios.
            cols_to_keep = list(filter(lambda x: not x.startswith("build_existing_model."), df.columns))
            df = df[cols_to_keep]
            null_cols = get_null_cols(df)
            # If certain column datatype is null (happens when it doesn't have any data), the datatype
            # for that column is attempted to be determined based on datatype in other upgrades
            if null_cols:
                logger.info(f"Upgrade {upgrade_id} has null cols: {null_cols}")
                schema, unresolved = correct_schema(all_schema_dict, df)
                if unresolved:
                    logger.info(f"The types for {unresolved} columns couldn't be determined.")
                else:
                    logger.info("All columns were successfully assigned a datatype based on other upgrades.")
        # Write CSV
        csv_filename = f"{results_csvs_dir}/results_up{upgrade_id:02d}.csv.gz"
        logger.info(f"Writing {csv_filename}")
        with fs.open(csv_filename, "wb") as f:
            with gzip.open(f, "wt", encoding="utf-8") as gf:
                df.to_csv(gf, index=True, lineterminator="\n")

        # Write Parquet
        if upgrade_id == 0:
            results_parquet_dir = f"{parquet_dir}/baseline"
        else:
            results_parquet_dir = f"{parquet_dir}/upgrades/upgrade={upgrade_id}"

        fs.makedirs(results_parquet_dir)
        parquet_filename = f"{results_parquet_dir}/results_up{upgrade_id:02d}.parquet"
        logger.info(f"Writing {parquet_filename}")
        write_dataframe_as_parquet(df.reset_index(), fs, parquet_filename, schema=schema)

        if do_timeseries:
            # Get the names of the timeseries file for each simulation in this upgrade
            ts_upgrade_path = f"{ts_in_dir}/up{upgrade_id:02d}"
            try:
                ts_filenames = [ts_upgrade_path + ts_filename for ts_filename in fs.ls(ts_upgrade_path)]
            except FileNotFoundError:
                # Upgrade directories may be empty if the upgrade is invalid. In some cloud
                # filesystems, there aren't actual directories, and trying to list a directory with
                # no files in it can fail. Just continue post-processing (other upgrades).
                logger.warning(f"Listing '{ts_upgrade_path}' failed. Skipping this upgrade.")
                continue
            ts_bldg_ids = [int(re.search(r"bldg(\d+).parquet", flname).group(1)) for flname in ts_filenames]
            if not ts_filenames:
                logger.warning(f"There are no timeseries files for upgrade{upgrade_id}.")
                continue
            logger.info(f"There are {len(ts_filenames)} timeseries files for upgrade{upgrade_id}.")

            # Calculate the mean and estimate the total memory usage
            read_ts_parquet = partial(read_enduse_timeseries_parquet, fs, all_ts_cols_sorted, ts_upgrade_path)
            get_ts_mem_usage_d = dask.delayed(lambda x: read_ts_parquet(x).memory_usage(deep=True).sum())
            sample_size = min(len(ts_bldg_ids), 36 * 3)
            mean_mem = np.mean(dask.compute(map(get_ts_mem_usage_d, random.sample(ts_bldg_ids, sample_size)))[0])

            # Determine how many files should be in each partition and group the files
            parquet_memory = int(
                cfg.get("eagle", {}).get("postprocessing", {}).get("parquet_memory_mb", MAX_PARQUET_MEMORY)
            )
            logger.info(f"Max parquet memory: {parquet_memory} MB")
            max_files_per_partition = max(1, math.floor(parquet_memory / (mean_mem / 1e6)))
            partition_df = partition_df.loc[ts_bldg_ids].copy()
            logger.info(f"partition_df for the upgrade has {len(partition_df)} rows.")
            bldg_id_groups, bldg_id_list, ngroup = get_partitioned_bldg_groups(
                partition_df, partition_columns, max_files_per_partition
            )
            logger.info(
                f"Processing {len(bldg_id_list)} building timeseries by combining max of "
                f"{max_files_per_partition} parquets together. This will create {len(bldg_id_groups)} parquet "
                f"partitions which go into {ngroup} column group(s) of {partition_columns}"
            )

            if isinstance(fs, LocalFileSystem):
                ts_out_loc = f"{ts_dir}/upgrade={upgrade_id}/"
            else:
                assert isinstance(fs, S3FileSystem)
                ts_out_loc = f"s3://{ts_dir}/upgrade={upgrade_id}/"

            fs.makedirs(ts_out_loc)
            logger.info(f"Created directory {ts_out_loc} for writing. Now concatenating ...")

            src_path = f"{ts_in_dir}/up{upgrade_id:02d}/"
            concat_partial = dask.delayed(
                partial(
                    concat_and_normalize,
                    fs,
                    all_ts_cols_sorted,
                    src_path,
                    ts_out_loc,
                    partition_columns,
                )
            )
            partition_vals_list = [
                list(partition_df.loc[bldg_id_list[0]].values) if partition_columns else []
                for bldg_id_list in bldg_id_groups
            ]

            with tempfile.TemporaryDirectory() as tmpdir:
                tmpfilepath = Path(tmpdir, "dask-report.html")
                with performance_report(filename=str(tmpfilepath)):
                    dask.compute(
                        map(
                            concat_partial,
                            *zip(*enumerate(bldg_id_groups)),
                            partition_vals_list,
                        )
                    )
                if tmpfilepath.exists():
                    fs.put_file(
                        str(tmpfilepath),
                        f"{results_dir}/dask_combine_report{upgrade_id}.html",
                    )

            logger.info(f"Finished combining and saving timeseries for upgrade{upgrade_id}.")
    logger.info("All aggregation completed. ")
    if do_timeseries:
        logger.info("Writing timeseries metadata files")
        write_metadata_files(fs, ts_dir, partition_columns)


def remove_intermediate_files(fs, results_dir, keep_individual_timeseries=False):
    # Remove aggregated files to save space
    sim_output_dir = f"{results_dir}/simulation_output"
    results_job_json_glob = f"{sim_output_dir}/results_job*.json.gz"
    logger.info("Removing results_job*.json.gz")
    for filename in fs.glob(results_job_json_glob):
        fs.rm(filename)
    if not keep_individual_timeseries:
        ts_in_dir = f"{sim_output_dir}/timeseries"
        fs.rm(ts_in_dir, recursive=True)


def upload_results(aws_conf, output_dir, results_dir, buildstock_csv_filename):
    logger.info("Uploading the parquet files to s3")

    output_folder_name = Path(output_dir).name
    parquet_dir = Path(results_dir).joinpath("parquet")
    ts_dir = parquet_dir / "timeseries"
    if not parquet_dir.is_dir():
        logger.error(f"{parquet_dir} does not exist. Please make sure postprocessing has been done.")
        raise FileNotFoundError(parquet_dir)

    all_files = []
    for file in parquet_dir.rglob("*.parquet"):
        all_files.append(file.relative_to(parquet_dir))
    for file in [*ts_dir.glob("_common_metadata"), *ts_dir.glob("_metadata")]:
        all_files.append(file.relative_to(parquet_dir))

    s3_prefix = aws_conf.get("s3", {}).get("prefix", "").rstrip("/")
    s3_bucket = aws_conf.get("s3", {}).get("bucket", None)
    if not (s3_prefix and s3_bucket):
        logger.error("YAML file missing postprocessing:aws:s3:prefix and/or bucket entry.")
        return
    s3_prefix_output = s3_prefix + "/" + output_folder_name + "/"

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_bucket)
    n_existing_files = len(list(bucket.objects.filter(Prefix=s3_prefix_output)))
    if n_existing_files > 0:
        logger.error(f"There are already {n_existing_files} files in the s3 folder {s3_bucket}/{s3_prefix_output}.")
        raise FileExistsError(f"s3://{s3_bucket}/{s3_prefix_output}")

    def upload_file(filepath, s3key=None):
        full_path = filepath if filepath.is_absolute() else parquet_dir.joinpath(filepath)
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(s3_bucket)
        if s3key is None:
            s3key = Path(s3_prefix_output).joinpath(filepath).as_posix()
        bucket.upload_file(str(full_path), str(s3key))

    tasks = list(map(dask.delayed(upload_file), all_files))
    if buildstock_csv_filename is not None:
        buildstock_csv_filepath = Path(buildstock_csv_filename)
        if buildstock_csv_filepath.exists():
            tasks.append(
                dask.delayed(upload_file)(
                    buildstock_csv_filepath,
                    f"{s3_prefix_output}buildstock_csv/{buildstock_csv_filepath.name}",
                )
            )
        else:
            logger.warning(f"{buildstock_csv_filename} doesn't exist, can't upload.")
    dask.compute(tasks)
    logger.info(f"Upload to S3 completed. The files are uploaded to: {s3_bucket}/{s3_prefix_output}")
    return s3_bucket, s3_prefix_output


def create_athena_tables(aws_conf, tbl_prefix, s3_bucket, s3_prefix):
    logger.info("Creating Athena tables using glue crawler")

    region_name = aws_conf.get("region_name", "us-west-2")
    db_name = aws_conf.get("athena", {}).get("database_name", None)
    role = aws_conf.get("athena", {}).get("glue_service_role", "service-role/AWSGlueServiceRole-default")
    max_crawling_time = aws_conf.get("athena", {}).get("max_crawling_time", 600)
    assert db_name, "athena:database_name not supplied"

    # Check that there are files in the s3 bucket before creating and running glue crawler
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_bucket)
    s3_path = f"s3://{s3_bucket}/{s3_prefix}"
    n_existing_files = len(list(bucket.objects.filter(Prefix=s3_prefix)))
    if n_existing_files == 0:
        logger.warning(f"There are no files in {s3_path}, Athena tables will not be created as intended")
        return

    glueClient = boto3.client("glue", region_name=region_name)
    crawlTarget = {
        "S3Targets": [{"Path": s3_path, "Exclusions": ["**_metadata", "**_common_metadata"], "SampleSize": 2}]
    }
    crawler_name = db_name + "_" + tbl_prefix
    tbl_prefix = tbl_prefix + "_"

    def create_crawler():
        glueClient.create_crawler(
            Name=crawler_name,
            Role=role,
            Targets=crawlTarget,
            DatabaseName=db_name,
            TablePrefix=tbl_prefix,
        )

    try:
        create_crawler()
    except glueClient.exceptions.AlreadyExistsException:
        logger.info(f"Deleting existing crawler: {crawler_name}. And creating new one.")
        glueClient.delete_crawler(Name=crawler_name)
        time.sleep(1)  # A small delay after deleting is required to prevent AlreadyExistsException again
        create_crawler()

    try:
        existing_tables = [x["Name"] for x in glueClient.get_tables(DatabaseName=db_name)["TableList"]]
    except glueClient.exceptions.EntityNotFoundException:
        existing_tables = []

    to_be_deleted_tables = [x for x in existing_tables if x.startswith(tbl_prefix)]
    if to_be_deleted_tables:
        logger.info(f"Deleting existing tables in db {db_name}: {to_be_deleted_tables}. And creating new ones.")
        glueClient.batch_delete_table(DatabaseName=db_name, TablesToDelete=to_be_deleted_tables)

    glueClient.start_crawler(Name=crawler_name)
    logger.info("Crawler started")
    start_time = time.time()
    elapsed_time = 0
    while elapsed_time < (3 * max_crawling_time):
        time.sleep(30)
        elapsed_time = time.time() - start_time
        crawler = glueClient.get_crawler(Name=crawler_name)["Crawler"]
        crawler_state = crawler["State"]
        logger.info(f"Crawler is {crawler_state}")
        if crawler_state == "RUNNING":
            if elapsed_time > max_crawling_time:
                logger.error("Crawler is taking too long. Aborting ...")
                glueClient.stop_crawler(Name=crawler_name)
        elif crawler_state == "STOPPING":
            logger.debug("Waiting for crawler to stop")
        else:
            assert crawler_state == "READY"
            metrics = glueClient.get_crawler_metrics(CrawlerNameList=[crawler_name])["CrawlerMetricsList"][0]
            logger.info(f"Crawler has completed running. It is {crawler_state}.")
            logger.info(
                f"TablesCreated: {metrics['TablesCreated']} "
                f"TablesUpdated: {metrics['TablesUpdated']} "
                f"TablesDeleted: {metrics['TablesDeleted']} "
            )
            break

    logger.info(f"Crawl {crawler['LastCrawl']['Status']}")
    logger.info(f"Deleting crawler {crawler_name}")
    try:
        glueClient.delete_crawler(Name=crawler_name)
    except botocore.exceptions.ClientError as error:
        logger.error(f"Could not delete crawler {crawler_name}. Please delete it manually from the AWS console.")
        raise error
