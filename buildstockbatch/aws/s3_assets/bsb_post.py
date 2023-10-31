import argparse
import boto3
from dask_yarn import YarnCluster
from dask.distributed import Client
import json
from s3fs import S3FileSystem

from postprocessing import (
    combine_results,
    create_athena_tables,
    remove_intermediate_files,
)


def do_postprocessing(s3_bucket, s3_bucket_prefix):
    fs = S3FileSystem()
    with fs.open(f"{s3_bucket}/{s3_bucket_prefix}/config.json", "r") as f:
        cfg = json.load(f)

    ec2 = boto3.client("ec2")

    with open("/mnt/var/lib/info/job-flow.json", "r") as f:
        job_flow_info = json.load(f)

    for instance_group in job_flow_info["instanceGroups"]:
        if instance_group["instanceRole"].lower() == "core":
            instance_type = instance_group["instanceType"]
            instance_count = instance_group["requestedInstanceCount"]

    instance_info = ec2.describe_instance_types(InstanceTypes=[instance_type])

    dask_worker_vcores = cfg["aws"].get("emr", {}).get("dask_worker_vcores", 2)
    instance_memory = instance_info["InstanceTypes"][0]["MemoryInfo"]["SizeInMiB"]
    instance_ncpus = instance_info["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
    n_dask_workers = instance_count * instance_ncpus // dask_worker_vcores
    worker_memory = round(instance_memory / instance_ncpus * dask_worker_vcores * 0.95)

    cluster = YarnCluster(
        deploy_mode="local",
        worker_vcores=dask_worker_vcores,
        worker_memory="{} MiB".format(worker_memory),
        n_workers=n_dask_workers,
    )

    client = Client(cluster)  # noqa E841

    results_s3_loc = f"{s3_bucket}/{s3_bucket_prefix}/results"

    combine_results(fs, results_s3_loc, cfg)

    aws_conf = cfg.get("postprocessing", {}).get("aws", {})
    if "athena" in aws_conf:
        tbl_prefix = s3_bucket_prefix.split("/")[-1]
        if not tbl_prefix:
            tbl_prefix = cfg["aws"]["job_identifier"]
        create_athena_tables(aws_conf, tbl_prefix, s3_bucket, f"{s3_bucket_prefix}/results/parquet")

    keep_individual_timeseries = cfg.get("postprocessing", {}).get("keep_individual_timeseries", False)
    remove_intermediate_files(fs, results_s3_loc, keep_individual_timeseries)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("s3_bucket")
    parser.add_argument("s3_bucket_prefix")
    args = parser.parse_args()
    do_postprocessing(args.s3_bucket, args.s3_bucket_prefix)
