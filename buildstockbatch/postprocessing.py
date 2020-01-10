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
from fs import open_fs
from fs.errors import ResourceNotFound, FileExpected
import gzip
import json
import logging
import numpy as np
import os
import random
import re
import sys
import time
import boto3
import pandas as pd
from pathlib import Path
import pyarrow as pa
from pyarrow import parquet
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)


def read_data_point_out_json(fs_uri, reporting_measures, filename):
    fs = open_fs(fs_uri)
    try:
        with fs.open(filename, 'r') as f:
            d = json.load(f)
    except (ResourceNotFound, FileExpected, json.JSONDecodeError):
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

    new_d['building_id'] = new_d['BuildExistingModel.building_id']
    del new_d['BuildExistingModel.building_id']

    return new_d


def read_out_osw(fs_uri, filename):
    fs = open_fs(fs_uri)
    try:
        with fs.open(filename, 'r') as f:
            d = json.load(f)
    except (ResourceNotFound, FileExpected, json.JSONDecodeError):
        return None
    else:
        out_d = {}
        keys_to_copy = [
            'started_at',
            'completed_at',
            'completed_status'
        ]
        for key in keys_to_copy:
            out_d[key] = d[key]
        for step in d['steps']:
            if step['measure_dir_name'] == 'BuildExistingModel':
                out_d['building_id'] = step['arguments']['building_id']
        return out_d


def write_dataframe_as_parquet(df, fs_uri, filename):
    fs = open_fs(fs_uri)
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(filename, 'wb') as f:
        parquet.write_table(tbl, f, flavor='spark')


def bldg_group(group_size, directory_name):
    m = re.search(r'up(\d+)/bldg(\d+)', directory_name)
    assert m, f"list of directories passed should be properly formatted as: " \
        f"'up([0-9]+)*bldg([0-9]+)'. Got {directory_name}"
    upgrade_id, building_id = m.groups()
    return f'up{upgrade_id}_Group{int(building_id) // group_size}'


def directory_name_append(name1, name2):
    if name1 is None:
        return name2
    else:
        return name1 + '\n' + name2


def add_timeseries(results_dir, inp1, inp2):
    fs = open_fs(results_dir)

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
        except ResourceNotFound:
            file1 = pd.DataFrame()  # if the timeseries file is missing, set it to empty dataframe
    else:
        file1 = inp1

    if type(inp2) is str:
        full_path = f"{inp2}/run/enduse_timeseries.parquet"
        try:
            with fs.open(full_path, 'rb') as f:
                file2 = pd.read_parquet(f, engine='pyarrow').set_index('Time')
                file2 = file2 * get_factor(inp2)
        except ResourceNotFound:
            file2 = pd.DataFrame()
    else:
        file2 = inp2

    return file1.add(file2, fill_value=0)


def write_output(results_dir, group_pq):
    fs = open_fs(results_dir)
    group = group_pq[0]
    folders = group_pq[1]

    try:
        upgrade, groupname = group.split('_')
        m = re.match(r'up(\d+)', upgrade)
        upgrade_id = int(m.group(1))
    except (ValueError, AttributeError):
        logger.error(f"The group labels created from bldg_group function should "
                     f"have 'up([0-9]+)_GroupXX' format. Found: {group}")
        return

    folder_path = f"parquet/timeseries/upgrade={upgrade_id}"
    file_path = f"{folder_path}/{groupname}.parquet"
    parquets = []
    for folder in folders.split():
        full_path = f"simulation_output/{folder}/run/enduse_timeseries.parquet"
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
        logger.warning(f'No valid simulation found for upgrade:{upgrade_id} and group:{groupname}.')
        logger.debug(f'The following folders were scanned {folders}.')
        return

    pq_size = (sum([sys.getsizeof(pq) for pq in parquets]) + sys.getsizeof(parquets)) / (1024 * 1024)
    logger.debug(f"{group}: list of {len(parquets)} parquets is consuming "
                 f"{pq_size:.2f} MB memory on a dask worker process.")
    pq = pd.concat(parquets)
    logger.debug(f"The concatenated parquet file is consuming {sys.getsizeof(pq) / (1024 * 1024) :.2f} MB.")
    write_dataframe_as_parquet(pq, results_dir, file_path)


def athena_return(year, ts_index, metadata):
    """
    Function to read the 15 minute time series parquet file, drop unneeded columns, & save to a new folder for gzip
    :param year: year to create athena parquet files for
    :param ts_index: Array of ISO8601 compliant interval timestamps
    :param metadata: dictionary of metadata to append to the timeseries data
    :return: Dictionary with id and success of the attempt to generate the athena parquet file
    """

    print('Processing the folder {}.'.format(metadata['building_id']))

    ts_name_mapping = {
        'Electricity:Facility__[kWh]': 'electricity_facility_kWh',
        'Gas:Facility__[kBtu]': 'gas_facility_kBtu',
        'Cooling:Electricity__[kWh]': 'cooling_electricity_kWh',
        'ExteriorLights:Electricity__[kWh]': 'exterior_lights_electricity_kWh',
        'Fans:Electricity__[kWh]': 'fans_electricity_kWh',
        'Heating:Electricity__[kWh]': 'heating_electricity_kWh',
        'InteriorEquipment:Electricity__[kWh]': 'interior_equipment_electricity_kWh',
        'InteriorEquipment:Gas__[kBtu]': 'interior_equipment_gas_kBtu',
        'Pumps:Electricity__[kWh]': 'pumps_electricity_kWh',
        'WaterSystems:Electricity__[kWh]': 'water_systems_electricity_kWh',
        'WaterSystems:Gas__[kBtu]': 'water_systems_gas_kBtu',
        'Water:Facility__[gal]': 'water_facility_gal',
        'Heating:Gas__[kBtu]': 'heating_gas_kBtu',
        'HeatRejection:Electricity__[kWh]': 'heat_rejection_electricity_kWh',
        'InteriorLights:Electricity__[kWh]': 'interior_lights_electricity_kWh',
        'Refrigeration:Electricity__[kWh]': 'refrigeration_electricity_kWh'
    }
    try:
        output_path = working_dir + 'up00/bldg{0:07d}/athena.parquet'.\
            format(metadata['building_id'])
        parquet_path = working_dir + 'up00/bldg{0:07d}/run/enduse_timeseries.parquet'.\
            format(metadata['building_id'])
        if not os.path.isfile(parquet_path):
            return {'id': metadata['building_id'], 'good': False}
        ts_df = pd.read_parquet(parquet_path, columns=ts_name_mapping.keys())
        for key in ts_name_mapping.keys():
            if key not in list(ts_df):
                ts_df.loc[:, key] = 0.0
        ts_df.columns = [ts_name_mapping[col] for col in ts_df.columns]
        for key in metadata:
            ts_df.loc[:, key] = metadata[key]
        ts_df.index = ts_index[0:data_len_dic[year]]
        new_order = [key for key in metadata.keys()]
        _ = [new_order.append(key) for key in ts_name_mapping.values()]
        ts_df = ts_df[new_order]
        ts_df.to_parquet(output_path)
        return {'id': metadata['building_id'], 'good': True}
    except pa.lib.ArrowIOError:
        return {'id': metadata['building_id'], 'good': False}


def concat_athena_parquets(chunck, index):
    to_concat = []
    for file in chunck:
        try:
            to_concat.append(pd.read_parquet(file))
        except pa.lib.ArrowIOError:
            continue
    to_save = pd.concat(to_concat)
    to_save.to_parquet(working_dir + 'athena_{}.parquet'.format(index))


def combine_results_ComStock(working_dir, results_dir, year, total_file_number):
    # year = '2012'
    # working_dir = '/projects/eedr/comstock/full-comstock/results/simulation_output/'
    # results_dir = '/projects/eedr/comstock/full-comstock/results/parquet/baseline'

    data_len_dic = {'2012': 35136,
                    '2013': 35040,
                    '2014': 35040,
                    '2015': 35040,
                    '2016': 35136,
                    '2017': 35040}

    if year not in ['2012', '2013', '2014', '2015', '2016', '2017']:
        raise RuntimeError('YEAR provided was {}'.format(year))
    # Iterate over the combination of scenarios and years in the LA100 study
    metadf = pd.read_parquet(results_dir + '/results_up00.parquet')
    # metadf = pd.read_parquet(results_dir + '/results_up00.parquet').iloc[0:20,:]
    metadf = metadf.loc[metadf['completed_status'] == 'Success']
    # metadf = pd.read_parquet(results_dir +'/results_up00.parquet')
    cols_to_keep = [
        'building_id',
        'build_existing_model.aspect_ratio',
        'build_existing_model.changebuildinglocation_climate_zone',
        'build_existing_model.changebuildinglocation_weather_file_name',
        'build_existing_model.create_bar_from_building_type_ratios_bldg_type_a',
        'build_existing_model.create_bar_from_building_type_ratios_floor_height',
        'build_existing_model.create_bar_from_building_type_ratios_num_stories_above_grade',
        'build_existing_model.create_bar_from_building_type_ratios_template',
        'build_existing_model.create_bar_from_building_type_ratios_total_bldg_floor_area',
        'build_existing_model.create_typical_building_from_model_system_type',
        'build_existing_model.envelope_code',
        'build_existing_model.ext_lgt_code',
        'build_existing_model.hvac_code',
        'build_existing_model.int_equip_code',
        'build_existing_model.int_lgt_code',
        'build_existing_model.swh_code',
        'simulation_output_report.hours_cooling_setpoint_not_met',
        'simulation_output_report.hours_heating_setpoint_not_met',
        'simulation_output_report.total_site_electricity_kwh',
        'simulation_output_report.total_site_natural_gas_therm'
    ]
    cols_to_drop = list(metadf)
    [cols_to_drop.remove(col) for col in list(metadf) if col in cols_to_keep]
    metadf = metadf.drop(cols_to_drop, axis=1)
    col_name_mapping = {
        'building_id': 'building_id',
        'build_existing_model.aspect_ratio': 'aspect_ratio',
        'build_existing_model.changebuildinglocation_climate_zone': 'cz',
        'build_existing_model.changebuildinglocation_weather_file_name': 'weather_file',
        'build_existing_model.create_bar_from_building_type_ratios_bldg_type_a': 'bldg_type',
        'build_existing_model.create_bar_from_building_type_ratios_floor_height': 'ftf_height',
        'build_existing_model.create_bar_from_building_type_ratios_num_stories_above_grade': 'ns',
        'build_existing_model.create_bar_from_building_type_ratios_template': 'built_code',
        'build_existing_model.create_bar_from_building_type_ratios_total_bldg_floor_area': 'sqft',
        'build_existing_model.create_typical_building_from_model_system_type': 'system_type',
        'build_existing_model.envelope_code': 'env_code',
        'build_existing_model.ext_lgt_code': 'ext_lgt_code',
        'build_existing_model.hvac_code': 'hvac_code',
        'build_existing_model.int_equip_code': 'int_equip_code',
        'build_existing_model.int_lgt_code': 'int_lgt_code',
        'build_existing_model.swh_code': 'swh_code',
        'simulation_output_report.hours_cooling_setpoint_not_met': 'clg_stp_not_met',
        'simulation_output_report.hours_heating_setpoint_not_met': 'htg_stp_not_met',
        'simulation_output_report.total_site_electricity_kwh': 'total_site_elec_kwh',
        'simulation_output_report.total_site_natural_gas_therm': 'total_site_gas_therm',
    }
    metadf.columns = [col_name_mapping[column] for column in metadf.columns]
    dask_bag_list = metadf.to_dict(orient='index')
    # Pre-instantiate the index that will be set on each parquet file
    start = dt.datetime(int(year), 1, 1, 0)
    intervals = [step * 15 for step in range(data_len_dic[year])]
    index = [start + dt.timedelta(minutes=interval) for interval in intervals]
    index = [timestamp.isoformat() for timestamp in index]

    # Create bag of dgen_return dictionaries to iterate over
    distro_results = db.\
        from_sequence(dask_bag_list, partition_size=500).\
        map(lambda x: dask_bag_list[x]).\
        map(lambda x: athena_return(year, index, x))
    all_res = compute(distro_results)
    # Document any failures - we need to deal with these somehow.
    failure_true_false_df = pd.DataFrame(all_res[0])
    failure_true_false_df.loc[failure_true_false_df.good == False].to_csv(working_dir + 'athena_pyarrow_failures.csv')
    print('Completed generating parquet files for each building folder.')

    # Step 2. aggregate parquet files
    # Aggregate individual athena files into total_file_number files
    # total_file_number = 140
    new_parquets = [
        working_dir + 'up00/bldg{0:07d}/athena.parquet'.format(dask_bag_list[item]['building_id']) for
        item in dask_bag_list
    ]
    random.shuffle(new_parquets)
    chunks = np.array_split(new_parquets, total_file_number)
    Parallel(n_jobs=-1, verbose=9)(
        delayed(concat_athena_parquets)(chunks[index], index) for index in range(len(chunks)))
    print('Completed aggregating athena parquet files for ComStock up00.')


def combine_results(results_dir, config, skip_timeseries=False, aggregate_timeseries=False, reporting_measures=[]):
    fs = open_fs(results_dir)

    sim_out_dir = 'simulation_output'
    results_csvs_dir = 'results_csvs'
    parquet_dir = 'parquet'
    ts_dir = 'parquet/timeseries'
    agg_ts_dir = 'parquet/aggregated_timeseries'
    dirs = [results_csvs_dir, parquet_dir]
    if not skip_timeseries:
        dirs += [ts_dir]
        if aggregate_timeseries:
            dirs += [agg_ts_dir]

    # clear and create the postprocessing results directories
    for dr in dirs:
        if fs.exists(dr):
            fs.removetree(dr)
        fs.makedirs(dr)

    sim_out_fs = fs.opendir(sim_out_dir)

    all_dirs = list()  # get the list of all the building simulation results directories
    results_by_upgrade = defaultdict(list)  # all the results directories, keyed by upgrades
    for item in sim_out_fs.listdir('.'):
        m = re.match(r'up(\d+)', item)
        if not m:
            continue
        upgrade_id = int(m.group(1))
        for subitem in sim_out_fs.listdir(item):
            m = re.match(r'bldg(\d+)', subitem)
            if not m:
                continue
            full_path = f"{item}/{subitem}"
            results_by_upgrade[upgrade_id].append(full_path)
            all_dirs.append(full_path)

    # create the results.csv and results.parquet files
    for upgrade_id, sim_dir_list in results_by_upgrade.items():

        logger.info('Computing results for upgrade {} with {} simulations'.format(upgrade_id, len(sim_dir_list)))

        datapoint_output_jsons = db.from_sequence(sim_dir_list, partition_size=500).\
            map(lambda x: f"{sim_out_dir}/{x}/run/data_point_out.json").\
            map(partial(read_data_point_out_json, results_dir, reporting_measures)).\
            filter(lambda x: x is not None)
        meta = pd.DataFrame(list(
            datapoint_output_jsons.filter(lambda x: 'SimulationOutputReport' in x.keys()).
            map(partial(flatten_datapoint_json, reporting_measures)).take(10)
        ))

        if meta.shape == (0, 0):
            meta = None

        data_point_out_df_d = datapoint_output_jsons.map(partial(flatten_datapoint_json, reporting_measures)).\
            to_dataframe(meta=meta).rename(columns=to_camelcase)

        out_osws = db.from_sequence(sim_dir_list, partition_size=500).\
            map(lambda x: f"{sim_out_dir}/{x}/out.osw")

        out_osw_df_d = out_osws.map(partial(read_out_osw, results_dir)).filter(lambda x: x is not None).to_dataframe()

        data_point_out_df, out_osw_df = dask.compute(data_point_out_df_d, out_osw_df_d)

        results_df = out_osw_df.merge(data_point_out_df, how='left', on='building_id')
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
            results_df[col] = results_df[col].map(lambda x: dt.datetime.strptime(x, '%Y%m%dT%H%M%SZ'))

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
        first_few_cols = ['building_id', 'started_at', 'completed_at', 'completed_status',
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
            results_dir,
            f"{results_parquet_dir}/results_up{upgrade_id:02d}.parquet"
        )

        # combine and save the aggregated timeseries file
        if not skip_timeseries and aggregate_timeseries:
            full_sim_dir = results_dir + '/' + sim_out_dir
            logger.info(f"Combining timeseries files for {upgrade_id} in direcotry {full_sim_dir}")
            agg_parquet = db.from_sequence(sim_dir_list).fold(partial(add_timeseries, full_sim_dir)).compute()
            agg_parquet.reset_index(inplace=True)

            agg_parquet_dir = f"{agg_ts_dir}/upgrade={upgrade_id}"
            if not fs.exists(agg_parquet_dir):
                fs.makedirs(agg_parquet_dir)

            write_dataframe_as_parquet(agg_parquet,
                                       results_dir,
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
        full_path = f"{sim_out_dir}/{all_dirs[rnd_ts_index]}/run/enduse_timeseries.parquet"
        try:
            with fs.open(full_path, 'rb') as f:
                pq = pd.read_parquet(f, engine='pyarrow')
        except ResourceNotFound:
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

    logger.info(f"Each parquet file is {avg_parquet_size / (1024 * 1024) :.2f} in memory. \n" +
                f"Combining {group_size} of them together, so that the size in memory is around 1.5 GB")

    directory_bags = db.from_sequence(all_dirs).foldby(
        partial(bldg_group, group_size),
        directory_name_append,
        initial=None,
        combine=directory_name_append
    )
    bags = directory_bags.compute()
    logger.info("Combining the parquets")
    t = time.time()
    write_file = db.from_sequence(bags).map(partial(write_output, results_dir))
    write_file.compute()
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

    files_bag = db. \
        from_sequence(all_files, partition_size=500).map(upload_file)
    files_bag.compute()
    logger.info(f"Upload to S3 completed. The files are uploaded to: {s3_bucket}/{s3_prefix_output}")
    return s3_bucket, s3_prefix_output


def create_athena_tables(aws_conf, output_dir, s3_bucket, s3_prefix):
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
    if output_dir is None:
        output_folder_name = s3_prefix.split('/')[-1]
    else:
        output_folder_name = os.path.basename(output_dir)
    crawler_name = db_name+'_'+output_folder_name
    tbl_prefix = output_folder_name + '_'

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
