import os
import pathlib
import pytest
import shutil

from buildstockbatch.localdocker import LocalDockerBatch

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
def test_resstock_local_batch(project_filename, mocker):
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

    # FIXME: Find a better way to do this
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
    assert (out_path / "parquet" / "baseline" / "results_up00.parquet").exists()
    ts_pq_path = out_path / "parquet" / "timeseries"
    for upgrade_id in range(0, n_upgrades + 1):
        assert (ts_pq_path / f"upgrade={upgrade_id}" / "group0.parquet").exists()
        assert (out_path / "results_csvs" / f"results_up{upgrade_id:02d}.csv.gz").exists()
    assert (ts_pq_path / "_common_metadata").exists()
    assert (ts_pq_path / "_metadata").exists()

    shutil.rmtree(out_path)
