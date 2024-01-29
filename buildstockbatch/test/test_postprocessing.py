from fsspec.implementations.local import LocalFileSystem
import gzip
import json
import logging
import os
import pathlib
import re
import tarfile
import pytest
import shutil
from unittest.mock import patch, MagicMock

from buildstockbatch import postprocessing
from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.utils import get_project_configuration, read_csv

postprocessing.performance_report = MagicMock()


def test_report_additional_results_csv_columns(basic_residential_project_file):
    reporting_measures = ["ReportingMeasure1", "ReportingMeasure2"]
    project_filename, results_dir = basic_residential_project_file({"reporting_measures": reporting_measures})

    fs = LocalFileSystem()

    results_dir = pathlib.Path(results_dir)
    sim_out_dir = results_dir / "simulation_output"
    with tarfile.open(sim_out_dir / "simulations_job0.tar.gz", "r") as tarf:
        tarf.extractall(sim_out_dir)

    dpouts2 = []
    for filename in sim_out_dir.rglob("data_point_out.json"):
        with filename.open("rt", encoding="utf-8") as f:
            dpout = json.load(f)
        dpout["ReportingMeasure1"] = {"column_1": 1, "column_2": 2}
        dpout["ReportingMeasure2"] = {"column_3": 3, "column_4": 4}
        with filename.open("wt", encoding="utf-8") as f:
            json.dump(dpout, f)

        sim_dir = str(filename.parent.parent)
        upgrade_id = int(re.search(r"up(\d+)", sim_dir).group(1))
        building_id = int(re.search(r"bldg(\d+)", sim_dir).group(1))
        dpouts2.append(postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, building_id))

    with gzip.open(sim_out_dir / "results_job0.json.gz", "wt", encoding="utf-8") as f:
        json.dump(dpouts2, f)

    cfg = get_project_configuration(project_filename)

    postprocessing.combine_results(fs, results_dir, cfg, do_timeseries=False)

    for upgrade_id in (0, 1):
        df = read_csv(str(results_dir / "results_csvs" / f"results_up{upgrade_id:02d}.csv.gz"))
        assert (df["reporting_measure1.column_1"] == 1).all()
        assert (df["reporting_measure1.column_2"] == 2).all()
        assert (df["reporting_measure2.column_3"] == 3).all()
        assert (df["reporting_measure2.column_4"] == 4).all()


def test_empty_results_assertion(basic_residential_project_file, capsys):
    project_filename, results_dir = basic_residential_project_file({})

    fs = LocalFileSystem()
    results_dir = pathlib.Path(results_dir)
    sim_out_dir = results_dir / "simulation_output"
    shutil.rmtree(sim_out_dir)  # no results
    cfg = get_project_configuration(project_filename)

    with pytest.raises(ValueError, match=r"No simulation results found to post-process"):
        assert postprocessing.combine_results(fs, results_dir, cfg, do_timeseries=False)


def test_large_parquet_combine(basic_residential_project_file):
    # Test a simulated scenario where the individual timeseries parquet are larger than the max memory per partition
    # allocated for the parquet file combining.

    project_filename, results_dir = basic_residential_project_file()

    with patch.object(BuildStockBatchBase, "weather_dir", None), patch.object(
        BuildStockBatchBase, "get_dask_client"
    ), patch.object(BuildStockBatchBase, "results_dir", results_dir), patch.object(
        postprocessing, "MAX_PARQUET_MEMORY", 1
    ):  # set the max memory to just 1MB
        bsb = BuildStockBatchBase(project_filename)
        bsb.process_results()  # this would raise exception if the postprocessing could not handle the situation


@pytest.mark.parametrize("keep_individual_timeseries", [True, False])
def test_keep_individual_timeseries(keep_individual_timeseries, basic_residential_project_file, mocker):
    project_filename, results_dir = basic_residential_project_file(
        {"postprocessing": {"keep_individual_timeseries": keep_individual_timeseries}}
    )

    mocker.patch.object(BuildStockBatchBase, "weather_dir", None)
    mocker.patch.object(BuildStockBatchBase, "get_dask_client")
    mocker.patch.object(BuildStockBatchBase, "results_dir", results_dir)
    bsb = BuildStockBatchBase(project_filename)
    bsb.process_results()

    results_path = pathlib.Path(results_dir)
    simout_path = results_path / "simulation_output"
    assert len(list(simout_path.glob("results_job*.json.gz"))) == 0

    ts_path = simout_path / "timeseries"
    assert ts_path.exists() == keep_individual_timeseries


def test_upgrade_missing_ts(basic_residential_project_file, mocker, caplog):
    caplog.set_level(logging.WARNING, logger="buildstockbatch.postprocessing")

    project_filename, results_dir = basic_residential_project_file()
    results_path = pathlib.Path(results_dir)
    for filename in (results_path / "simulation_output" / "timeseries" / "up01").glob("*.parquet"):
        os.remove(filename)

    mocker.patch.object(BuildStockBatchBase, "weather_dir", None)
    mocker.patch.object(BuildStockBatchBase, "get_dask_client")
    mocker.patch.object(BuildStockBatchBase, "results_dir", results_dir)
    bsb = BuildStockBatchBase(project_filename)
    bsb.process_results()

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelname == "WARNING"
    assert record.message == "There are no timeseries files for upgrade1."
