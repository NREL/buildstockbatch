import os
import pandas as pd
import pathlib
import pytest
import shutil
import tempfile
import json
import re

from buildstockbatch.localdocker import LocalDockerBatch
from buildstockbatch.utils import get_project_configuration
from buildstockbatch.exc import ValidationError

resstock_directory = pathlib.Path(
    os.environ.get("RESSTOCK_DIR", pathlib.Path(__file__).resolve().parent.parent.parent.parent / "resstock")
)
resstock_required = pytest.mark.skipif(
    not resstock_directory.exists(),
    reason="ResStock checkout is not found"
)


@pytest.mark.parametrize("project_filename", [
    resstock_directory / "project_national" / "national_baseline.yml",
    resstock_directory / "project_national" / "national_upgrades.yml",
    resstock_directory / "project_testing" / "testing_baseline.yml",
    resstock_directory / "project_testing" / "testing_upgrades.yml",
], ids=lambda x: x.stem)
@resstock_required
def test_resstock_local_batch(project_filename):
    LocalDockerBatch.validate_project(str(project_filename))
    batch = LocalDockerBatch(str(project_filename))

    # Get the number of upgrades
    n_upgrades = len(batch.cfg.get("upgrades", []))
    # Limit the number of upgrades to 2 to reduce simulation time
    if n_upgrades > 2:
        batch.cfg["upgrades"] = batch.cfg["upgrades"][0:2]
        n_upgrades = 2

    # Modify the number of datapoints so we're not here all day.
    if n_upgrades == 0:
        n_datapoints = 4
    else:
        n_datapoints = 2
    batch.cfg["sampler"]["args"]["n_datapoints"] = n_datapoints

    local_weather_file = resstock_directory.parent / "weather" / batch.cfg["weather_files_url"].split("/")[-1]
    if local_weather_file.exists():
        del batch.cfg["weather_files_url"]
        batch.cfg["weather_files_path"] = str(local_weather_file)

    batch.run_batch()

    # Make sure all the files are there
    out_path = pathlib.Path(batch.output_dir)
    simout_path = out_path / "simulation_output"
    assert (simout_path / "results_job0.json.gz").exists()
    assert (simout_path / "simulations_job0.tar.gz").exists()

    for upgrade_id in range(0, n_upgrades + 1):
        for bldg_id in range(1, n_datapoints + 1):
            assert (simout_path / "timeseries" / f"up{upgrade_id:02d}" / f"bldg{bldg_id:07d}.parquet").exists()

    batch.process_results()

    assert not (simout_path / "timeseries").exists()
    assert not (simout_path / "results_job0.json.gz").exists()
    assert (simout_path / "simulations_job0.tar.gz").exists()
    base_pq = out_path / "parquet" / "baseline" / "results_up00.parquet"
    assert base_pq.exists()
    base = pd.read_parquet(base_pq, columns=["completed_status"])
    assert (base["completed_status"] == "Success").all()
    assert base.shape[0] == n_datapoints
    ts_pq_path = out_path / "parquet" / "timeseries"
    for upgrade_id in range(0, n_upgrades + 1):
        assert (ts_pq_path / f"upgrade={upgrade_id}" / "group0.parquet").exists()
        assert (out_path / "results_csvs" / f"results_up{upgrade_id:02d}.csv.gz").exists()
        if upgrade_id >= 1:
            upg_pq = out_path / "parquet" / "upgrades" / f"upgrade={upgrade_id}" / f"results_up{upgrade_id:02d}.parquet"
            assert upg_pq.exists()
            upg = pd.read_parquet(upg_pq, columns=["completed_status"])
            assert (upg["completed_status"] == "Success").all()
            assert upg.shape[0] == n_datapoints
    assert (ts_pq_path / "_common_metadata").exists()
    assert (ts_pq_path / "_metadata").exists()

    shutil.rmtree(out_path)


@resstock_required
def test_number_of_options_apply_upgrade():
    proj_filename = resstock_directory / "project_national" / "national_upgrades.yml"
    cfg = get_project_configuration(str(proj_filename))
    cfg["upgrades"][-1]["options"] = cfg["upgrades"][-1]["options"] * 10
    cfg["upgrades"][0]["options"][0]["costs"] = cfg["upgrades"][0]["options"][0]["costs"] * 5
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = pathlib.Path(tmpdir)
        new_proj_filename = tmppath / "project.yml"
        with open(new_proj_filename, "w") as f:
            json.dump(cfg, f)
        with pytest.raises(ValidationError):
            LocalDockerBatch.validate_number_of_options(str(new_proj_filename))
