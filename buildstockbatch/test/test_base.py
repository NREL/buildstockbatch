import csv
import dask
from fsspec.implementations.local import LocalFileSystem
import gzip
import json
import numpy as np
import os
import pandas as pd
from pyarrow import parquet
import pytest
import re
import shutil
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock
import yaml
from pathlib import Path

import buildstockbatch
from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.local import LocalBatch
from buildstockbatch.exc import ValidationError
from buildstockbatch.postprocessing import write_dataframe_as_parquet
from buildstockbatch.utils import read_csv, ContainerRuntime

dask.config.set(scheduler="synchronous")
here = os.path.dirname(os.path.abspath(__file__))

OUTPUT_FOLDER_NAME = "output"

buildstockbatch.postprocessing.performance_report = MagicMock()


def test_reference_scenario(basic_residential_project_file):
    # verify that the reference_scenario get's added to the upgrade file

    upgrade_config = {
        "upgrades": [
            {
                "upgrade_name": "Triple-Pane Windows",
                "reference_scenario": "example_reference_scenario",
            }
        ]
    }
    project_filename, results_dir = basic_residential_project_file(upgrade_config)

    with patch.object(BuildStockBatchBase, "weather_dir", None), patch.object(
        BuildStockBatchBase, "get_dask_client"
    ) as get_dask_client_mock, patch.object(BuildStockBatchBase, "results_dir", results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    # test results.csv files
    test_path = os.path.join(results_dir, "results_csvs")
    test_csv = read_csv(os.path.join(test_path, "results_up01.csv.gz")).set_index("building_id").sort_index()
    assert len(test_csv["apply_upgrade.reference_scenario"].unique()) == 1
    assert test_csv["apply_upgrade.reference_scenario"].iloc[0] == "example_reference_scenario"


def test_downselect_integer_options(basic_residential_project_file, mocker):
    with tempfile.TemporaryDirectory() as buildstock_csv_dir:
        buildstock_csv = os.path.join(buildstock_csv_dir, "buildstock.csv")
        valid_option_values = set()
        with open(os.path.join(here, "buildstock.csv"), "r", newline="") as f_in, open(
            buildstock_csv, "w", newline=""
        ) as f_out:
            cf_in = csv.reader(f_in)
            cf_out = csv.writer(f_out)
            for i, row in enumerate(cf_in):
                if i == 0:
                    col_idx = row.index("Days Shifted")
                else:
                    # Convert values from "Day1" to "1.10" so we hit the bug
                    row[col_idx] = "{0}.{0}0".format(re.search(r"Day(\d+)", row[col_idx]).group(1))
                    valid_option_values.add(row[col_idx])
                cf_out.writerow(row)

        project_filename, results_dir = basic_residential_project_file(
            {
                "sampler": {
                    "type": "residential_quota_downselect",
                    "args": {
                        "n_datapoints": 8,
                        "resample": False,
                        "logic": "Geometry House Size|1500-2499",
                    },
                }
            }
        )
        mocker.patch.object(BuildStockBatchBase, "weather_dir", None)
        mocker.patch.object(BuildStockBatchBase, "results_dir", results_dir)
        sampler_property_mock = mocker.patch.object(BuildStockBatchBase, "sampler", new_callable=PropertyMock)
        sampler_mock = mocker.MagicMock()
        sampler_property_mock.return_value = sampler_mock
        sampler_mock.run_sampling = MagicMock(return_value=buildstock_csv)

        bsb = BuildStockBatchBase(project_filename)
        bsb.sampler.run_sampling()
        sampler_mock.run_sampling.assert_called_once()
        with open(buildstock_csv, "r", newline="") as f:
            cf = csv.DictReader(f)
            for row in cf:
                assert row["Days Shifted"] in valid_option_values


@patch("buildstockbatch.postprocessing.boto3")
def test_upload_files(mocked_boto3, basic_residential_project_file):
    s3_bucket = "test_bucket"
    s3_prefix = "test_prefix"
    db_name = "test_db_name"
    role = "test_role"
    region = "test_region"

    upload_config = {
        "postprocessing": {
            "aws": {
                "region_name": region,
                "s3": {
                    "bucket": s3_bucket,
                    "prefix": s3_prefix,
                },
                "athena": {
                    "glue_service_role": role,
                    "database_name": db_name,
                    "max_crawling_time": 250,
                },
            }
        }
    }
    mocked_glueclient = MagicMock()
    mocked_glueclient.get_crawler = MagicMock(return_value={"Crawler": {"State": "READY"}})
    mocked_boto3.client = MagicMock(return_value=mocked_glueclient)
    mocked_boto3.resource().Bucket().objects.filter.side_effect = [[], ["a", "b", "c"]]
    project_filename, results_dir = basic_residential_project_file(upload_config)
    buildstock_csv_path = (
        Path(results_dir).parent
        / "openstudio_buildstock"
        / "project_resstock_national"
        / "housing_characteristics"
        / "buildstock.csv"
    )  # noqa: E501
    shutil.copy2(
        Path(__file__).parent / "test_results" / "housing_characteristics" / "buildstock.csv",
        buildstock_csv_path,
    )
    with patch.object(BuildStockBatchBase, "weather_dir", None), patch.object(
        BuildStockBatchBase, "output_dir", results_dir
    ), patch.object(BuildStockBatchBase, "get_dask_client") as get_dask_client_mock, patch.object(
        BuildStockBatchBase, "results_dir", results_dir
    ), patch.object(
        BuildStockBatchBase, "CONTAINER_RUNTIME", ContainerRuntime.LOCAL_OPENSTUDIO
    ):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    files_uploaded = []
    crawler_created = False
    crawler_started = False
    for call in mocked_boto3.mock_calls[2:] + mocked_boto3.client().mock_calls:
        call_function = call[0].split(".")[-1]  # 0 is for the function name
        if call_function == "resource":
            assert call[1][0] in ["s3"]  # call[1] is for the positional arguments
        if call_function == "Bucket":
            assert call[1][0] == s3_bucket
        if call_function == "upload_file":
            source_file_path = call[1][0]
            destination_path = call[1][1]
            files_uploaded.append((source_file_path, destination_path))
        if call_function == "create_crawler":
            crawler_para = call[2]  # 2 is for the keyword arguments
            crawler_created = True
            assert crawler_para["DatabaseName"] == upload_config["postprocessing"]["aws"]["athena"]["database_name"]
            assert crawler_para["Role"] == upload_config["postprocessing"]["aws"]["athena"]["glue_service_role"]
            assert crawler_para["TablePrefix"] == OUTPUT_FOLDER_NAME + "_"
            assert crawler_para["Name"] == db_name + "_" + OUTPUT_FOLDER_NAME
            assert (
                crawler_para["Targets"]["S3Targets"][0]["Path"]
                == "s3://" + s3_bucket + "/" + s3_prefix + "/" + OUTPUT_FOLDER_NAME + "/"
            )
        if call_function == "start_crawler":
            assert crawler_created, "crawler attempted to start before creating"
            crawler_started = True
            crawler_para = call[2]  # 2 is for keyboard arguments.
            assert crawler_para["Name"] == db_name + "_" + OUTPUT_FOLDER_NAME

    assert crawler_started, "Crawler never started"

    # check if all the files are properly uploaded
    source_path = os.path.join(results_dir, "parquet")
    s3_path = s3_prefix + "/" + OUTPUT_FOLDER_NAME + "/"

    s3_file_path = s3_path + "baseline/results_up00.parquet"
    source_file_path = os.path.join(source_path, "baseline", "results_up00.parquet")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "upgrades/upgrade=1/results_up01.parquet"
    source_file_path = os.path.join(source_path, "upgrades", "upgrade=1", "results_up01.parquet")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "timeseries/upgrade=0/group0.parquet"
    source_file_path = os.path.join(source_path, "timeseries", "upgrade=0", "group0.parquet")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "timeseries/upgrade=1/group0.parquet"
    source_file_path = os.path.join(source_path, "timeseries", "upgrade=1", "group0.parquet")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "timeseries/_common_metadata"
    source_file_path = os.path.join(source_path, "timeseries", "_common_metadata")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "timeseries/_metadata"
    source_file_path = os.path.join(source_path, "timeseries", "_metadata")
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    s3_file_path = s3_path + "buildstock_csv/buildstock.csv"
    source_file_path = str(buildstock_csv_path)
    assert (source_file_path, s3_file_path) in files_uploaded
    files_uploaded.remove((source_file_path, s3_file_path))

    assert len(files_uploaded) == 0, f"These files shouldn't have been uploaded: {files_uploaded}"


def test_write_parquet_no_index():
    df = pd.DataFrame(np.random.randn(6, 4), columns=list("abcd"), index=np.arange(6))

    with tempfile.TemporaryDirectory() as tmpdir:
        fs = LocalFileSystem()
        filename = os.path.join(tmpdir, "df.parquet")
        write_dataframe_as_parquet(df, fs, filename)
        schema = parquet.read_schema(os.path.join(tmpdir, filename))
        assert "__index_level_0__" not in schema.names
        assert df.columns.values.tolist() == schema.names


def test_skipping_baseline(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file(
        {"baseline": {"skip_sims": True, "sampling_algorithm": "quota"}}
    )

    sim_output_path = os.path.join(results_dir, "simulation_output")
    shutil.rmtree(os.path.join(sim_output_path, "timeseries", "up00"))  # remove timeseries results for baseline

    # remove results.csv data for baseline from results_jobx.json.gz
    results_json_filename = os.path.join(sim_output_path, "results_job0.json.gz")
    with gzip.open(results_json_filename, "rt", encoding="utf-8") as f:
        dpouts = json.load(f)
    dpouts2 = list(filter(lambda x: x["upgrade"] > 0, dpouts))
    with gzip.open(results_json_filename, "wt", encoding="utf-8") as f:
        json.dump(dpouts2, f)

    # remove jobs for baseline from jobx.json
    with open(os.path.join(results_dir, "..", "job0.json"), "rt") as f:
        job_json = json.load(f)
    job_json["batch"] = list(filter(lambda job: job[1] is not None, job_json["batch"]))
    with open(os.path.join(results_dir, "..", "job0.json"), "wt") as f:
        json.dump(job_json, f)

    # run postprocessing
    with patch.object(BuildStockBatchBase, "weather_dir", None), patch.object(
        BuildStockBatchBase, "get_dask_client"
    ) as get_dask_client_mock, patch.object(BuildStockBatchBase, "results_dir", results_dir):
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()
        get_dask_client_mock.assert_called_once()

    up00_parquet = os.path.join(results_dir, "parquet", "baseline", "results_up00.parquet")
    assert not os.path.exists(up00_parquet)

    up01_parquet = os.path.join(results_dir, "parquet", "upgrades", "upgrade=1", "results_up01.parquet")
    assert os.path.exists(up01_parquet)

    up00_csv_gz = os.path.join(results_dir, "results_csvs", "results_up00.csv.gz")
    assert not os.path.exists(up00_csv_gz)

    up01_csv_gz = os.path.join(results_dir, "results_csvs", "results_up01.csv.gz")
    assert os.path.exists(up01_csv_gz)


def test_provide_buildstock_csv(basic_residential_project_file, mocker):
    buildstock_csv = os.path.join(here, "buildstock.csv")
    df = read_csv(buildstock_csv, dtype=str)
    project_filename, results_dir = basic_residential_project_file(
        {"sampler": {"type": "precomputed", "args": {"sample_file": buildstock_csv}}}
    )
    mocker.patch.object(LocalBatch, "weather_dir", None)
    mocker.patch.object(LocalBatch, "results_dir", results_dir)

    bsb = LocalBatch(project_filename)
    sampling_output_csv = bsb.sampler.run_sampling()
    df2 = read_csv(sampling_output_csv, dtype=str)
    pd.testing.assert_frame_equal(df, df2)
    assert (df["Geometry Shared Walls"] == "None").all()  # Verify None is being read properly
    # Test file missing
    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["sampler"]["args"]["sample_file"] = os.path.join(here, "non_existant_file.csv")
    with open(project_filename, "w") as f:
        yaml.dump(cfg, f)

    with pytest.raises(ValidationError, match=r"sample_file doesn't exist"):
        LocalBatch(project_filename).sampler.run_sampling()
