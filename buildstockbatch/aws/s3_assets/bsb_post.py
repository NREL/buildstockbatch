import argparse
from dask_yarn import YarnCluster
from dask.distributed import Client
import json
from s3fs import S3FileSystem

from postprocessing import combine_results, create_athena_tables, remove_intermediate_files


def do_postprocessing(s3_bucket, s3_bucket_prefix):

    fs = S3FileSystem()
    with fs.open(f'{s3_bucket}/{s3_bucket_prefix}/config.json', 'r') as f:
        cfg = json.load(f)
    with fs.open(f'{s3_bucket}/{s3_bucket_prefix}/emr/bsb_post_config.json', 'r') as f:
        bsb_post_cfg = json.load(f)

    cluster = YarnCluster(
        deploy_mode='local',
        worker_vcores=bsb_post_cfg['emr_dask_worker_vcores'],
        worker_memory=bsb_post_cfg['worker_memory'] + ' MiB',
        n_workers=bsb_post_cfg['n_dask_workers']
    )

    client = Client(cluster)  # noqa E841

    results_s3_loc = f'{s3_bucket}/{s3_bucket_prefix}/results'

    combine_results(fs, results_s3_loc, cfg)

    aws_conf = cfg.get('postprocessing', {}).get('aws', {})
    if 'athena' in aws_conf:
        create_athena_tables(
            aws_conf,
            bsb_post_cfg['tbl_prefix'],
            s3_bucket,
            f'{s3_bucket_prefix}/results/parquet'
        )

    remove_intermediate_files(fs, results_s3_loc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('s3_bucket')
    parser.add_argument('s3_bucket_prefix')
    args = parser.parse_args()
    do_postprocessing(args.s3_bucket, args.s3_bucket_prefix)
