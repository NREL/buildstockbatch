
from dask_yarn import YarnCluster
from dask.distributed import Client

cluster = YarnCluster(
    deploy_mode='local',
    worker_vcores=2,
    worker_memory='15 GB',
    n_workers=16
)

client = Client(cluster)
from buildstockbatch.postprocessing import combine_results, create_athena_tables
results_s3_loc = 's3://buildstockbatch-test3/noeltest22pref/results/'
full_path = 'simulation_output/up01/bldg0000563/run/enduse_timeseries.parquet'

from fs import open_fs
import pandas as pd
import os
from fs.copy import copy_file

s3fs = open_fs(results_s3_loc)

s3fs.getinfo(full_path).is_file

with s3fs.open(full_path, 'rb') as f:
    df = pd.read_parquet(f, engine='pyarrow')

combine_results(results_s3_loc)

create_athena_tables(None, buildstockbatch-test3, 'noeltest22pref/results/')

