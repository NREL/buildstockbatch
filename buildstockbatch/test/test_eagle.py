import joblib
import json
import os
import pandas as pd
import pathlib
import shutil
import tarfile
from unittest.mock import patch
import gzip

from buildstockbatch.eagle import user_cli, EagleBatch
from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.utils import get_project_configuration, read_csv

here = os.path.dirname(os.path.abspath(__file__))


@patch("buildstockbatch.eagle.subprocess")
def test_hpc_run_building(mock_subprocess, monkeypatch, basic_residential_project_file):
    tar_filename = (
        pathlib.Path(__file__).resolve().parent / "test_results" / "simulation_output" / "simulations_job0.tar.gz"
    )  # noqa E501
    with tarfile.open(tar_filename, "r") as tarf:
        osw_dict = json.loads(tarf.extractfile("up00/bldg0000001/in.osw").read().decode("utf-8"))

    project_filename, results_dir = basic_residential_project_file()
    tmp_path = pathlib.Path(results_dir).parent
    sim_path = tmp_path / "output" / "simulation_output" / "up00" / "bldg0000001"
    os.makedirs(sim_path)

    cfg = get_project_configuration(project_filename)

    with patch.object(EagleBatch, "weather_dir", None), patch.object(
        EagleBatch, "create_osw", return_value=osw_dict
    ), patch.object(EagleBatch, "make_sim_dir", return_value=("bldg0000001up00", sim_path)), patch.object(
        EagleBatch, "local_scratch", tmp_path
    ):
        # Normal run
        run_bldg_args = [results_dir, cfg, 1, None]
        EagleBatch.run_building(*run_bldg_args)
        expected_singularity_args = [
            "singularity",
            "exec",
            "--contain",
            "-e",
            "--pwd",
            "/var/simdata/openstudio",
        ]
        end_expected_singularity_args = [
            str(pathlib.Path("/tmp/scratch/openstudio.simg")),
            "bash",
            "-x",
        ]
        mock_subprocess.run.assert_called_once()
        args = mock_subprocess.run.call_args[0][0]
        for a, b in [args[i : i + 2] for i in range(6, len(args) - 3, 2)]:
            assert a == "-B"
            drive, tail = os.path.splitdrive(b)
            assert tail.split(":")[1] in (
                "/var/simdata/openstudio",
                "/lib/resources",
                "/lib/housing_characteristics",
                "/measures",
                "/weather",
                "/tmp",
            )
        assert mock_subprocess.run.call_args[0][0][0:6] == expected_singularity_args
        assert mock_subprocess.run.call_args[0][0][-3:] == end_expected_singularity_args
        called_kw = mock_subprocess.run.call_args[1]
        assert called_kw.get("check") is True
        assert "input" in called_kw
        assert "stdout" in called_kw
        assert "stderr" in called_kw
        assert str(called_kw.get("cwd")) == str(pathlib.Path("/tmp/scratch/output"))
        assert called_kw["input"].decode("utf-8").find(" --measures_only") == -1

        # Measures only run
        mock_subprocess.reset_mock()
        shutil.rmtree(sim_path)
        os.makedirs(sim_path)
        monkeypatch.setenv("MEASURESONLY", "1")
        EagleBatch.run_building(*run_bldg_args)
        mock_subprocess.run.assert_called_once()
        assert mock_subprocess.run.call_args[0][0][0:6] == expected_singularity_args
        assert mock_subprocess.run.call_args[0][0][-3:] == end_expected_singularity_args
        called_kw = mock_subprocess.run.call_args[1]
        assert called_kw.get("check") is True
        assert "input" in called_kw
        assert "stdout" in called_kw
        assert "stderr" in called_kw
        assert str(called_kw.get("cwd")) == str(pathlib.Path("/tmp/scratch/output"))
        assert called_kw["input"].decode("utf-8").find(" --measures_only") > -1


@patch("buildstockbatch.base.BuildStockBatchBase.validate_options_lookup")
@patch("buildstockbatch.eagle.EagleBatch.validate_output_directory_eagle")
@patch("buildstockbatch.eagle.EagleBatch.validate_singularity_image_eagle")
@patch("buildstockbatch.eagle.subprocess")
def test_user_cli(
    mock_subprocess,
    mock_validate_singularity_image_eagle,
    mock_validate_output_directory_eagle,
    mock_validate_options,
    basic_residential_project_file,
    monkeypatch,
):
    mock_validate_options.return_value = True
    mock_validate_output_directory_eagle.return_value = True
    mock_validate_singularity_image_eagle.return_value = True

    project_filename, results_dir = basic_residential_project_file()
    shutil.rmtree(results_dir)
    monkeypatch.setenv("CONDA_PREFIX", "something")
    argv = [project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    eagle_sh = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "eagle.sh"))
    assert mock_subprocess.run.call_args[0][0][-1] == eagle_sh
    assert "--time=20" in mock_subprocess.run.call_args[0][0]
    assert "--account=testaccount" in mock_subprocess.run.call_args[0][0]
    assert "--nodes=1" in mock_subprocess.run.call_args[0][0]
    assert "--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY" in mock_subprocess.run.call_args[0][0]
    assert "--output=sampling.out" in mock_subprocess.run.call_args[0][0]
    assert "--qos=high" not in mock_subprocess.run.call_args[0][0]
    assert "0" == mock_subprocess.run.call_args[1]["env"]["MEASURESONLY"]

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ["--hipri", project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert "--time=20" in mock_subprocess.run.call_args[0][0]
    assert "--account=testaccount" in mock_subprocess.run.call_args[0][0]
    assert "--nodes=1" in mock_subprocess.run.call_args[0][0]
    assert "--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY" in mock_subprocess.run.call_args[0][0]
    assert "--output=sampling.out" in mock_subprocess.run.call_args[0][0]
    assert "--qos=high" in mock_subprocess.run.call_args[0][0]
    assert "0" == mock_subprocess.run.call_args[1]["env"]["MEASURESONLY"]
    assert "0" == mock_subprocess.run.call_args[1]["env"]["SAMPLINGONLY"]

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ["--measures_only", project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert "--time=20" in mock_subprocess.run.call_args[0][0]
    assert "--account=testaccount" in mock_subprocess.run.call_args[0][0]
    assert "--nodes=1" in mock_subprocess.run.call_args[0][0]
    assert "--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY" in mock_subprocess.run.call_args[0][0]
    assert "--output=sampling.out" in mock_subprocess.run.call_args[0][0]
    assert "--qos=high" not in mock_subprocess.run.call_args[0][0]
    assert "1" == mock_subprocess.run.call_args[1]["env"]["MEASURESONLY"]
    assert "0" == mock_subprocess.run.call_args[1]["env"]["SAMPLINGONLY"]

    mock_subprocess.reset_mock()
    shutil.rmtree(results_dir)
    argv = ["--samplingonly", project_filename]
    user_cli(argv)
    mock_subprocess.run.assert_called_once()
    assert "--time=20" in mock_subprocess.run.call_args[0][0]
    assert "--account=testaccount" in mock_subprocess.run.call_args[0][0]
    assert "--nodes=1" in mock_subprocess.run.call_args[0][0]
    assert "--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY" in mock_subprocess.run.call_args[0][0]
    assert "--output=sampling.out" in mock_subprocess.run.call_args[0][0]
    assert "--qos=high" not in mock_subprocess.run.call_args[0][0]
    assert "1" == mock_subprocess.run.call_args[1]["env"]["SAMPLINGONLY"]
    assert "0" == mock_subprocess.run.call_args[1]["env"]["MEASURESONLY"]


@patch("buildstockbatch.eagle.subprocess")
def test_qos_high_job_submit(mock_subprocess, basic_residential_project_file, monkeypatch):
    mock_subprocess.run.return_value.stdout = "Submitted batch job 1\n"
    mock_subprocess.PIPE = None
    project_filename, results_dir = basic_residential_project_file()
    shutil.rmtree(results_dir)
    monkeypatch.setenv("CONDA_PREFIX", "something")
    monkeypatch.setenv("SLURM_JOB_QOS", "high")

    with patch.object(EagleBatch, "weather_dir", None):
        batch = EagleBatch(project_filename)
        for i in range(1, 11):
            pathlib.Path(results_dir, "job{:03d}.json".format(i)).touch()
        with open(os.path.join(results_dir, "job001.json"), "w") as f:
            json.dump({"batch": list(range(100))}, f)
        batch.queue_jobs()
        mock_subprocess.run.assert_called_once()
        assert "--qos=high" in mock_subprocess.run.call_args[0][0]

    mock_subprocess.reset_mock()
    mock_subprocess.run.return_value.stdout = "Submitted batch job 1\n"
    mock_subprocess.PIPE = None

    with patch.object(EagleBatch, "weather_dir", None):
        batch = EagleBatch(project_filename)
        batch.queue_post_processing()
        mock_subprocess.run.assert_called_once()
        assert "--qos=high" in mock_subprocess.run.call_args[0][0]


def test_queue_jobs_minutes_per_sim(mocker, basic_residential_project_file, monkeypatch):
    mock_subprocess = mocker.patch("buildstockbatch.eagle.subprocess")
    mocker.patch.object(EagleBatch, "weather_dir", None)
    mock_subprocess.run.return_value.stdout = "Submitted batch job 1\n"
    mock_subprocess.PIPE = None
    project_filename, results_dir = basic_residential_project_file(
        update_args={
            "eagle": {
                "sampling": {"time": 20},
                "account": "testaccount",
                "minutes_per_sim": 0.5,
            }
        }
    )
    shutil.rmtree(results_dir)
    monkeypatch.setenv("CONDA_PREFIX", "something")

    batch = EagleBatch(project_filename)
    for i in range(1, 11):
        pathlib.Path(results_dir, "job{:03d}.json".format(i)).touch()
    with open(os.path.join(results_dir, "job001.json"), "w") as f:
        json.dump({"batch": list(range(1000))}, f)
    batch.queue_jobs()
    mock_subprocess.run.assert_called_once()
    assert "--time=14" in mock_subprocess.run.call_args[0][0]


def test_run_building_process(mocker, basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file(raw=True)
    results_dir = pathlib.Path(results_dir)

    job_json = {
        "job_num": 1,
        "batch": [
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 0),
            (1, None),
            (2, None),
            (3, None),
            (4, None),
        ],
        "n_datapoints": 8,
    }
    with open(results_dir / "job001.json", "w") as f:
        json.dump(job_json, f)

    sample_buildstock_csv = pd.DataFrame.from_records([{"Building": i, "Dummy Column": i * i} for i in range(10)])
    os.makedirs(results_dir / "housing_characteristics", exist_ok=True)
    os.makedirs(results_dir / "weather", exist_ok=True)
    sample_buildstock_csv.to_csv(results_dir / "housing_characteristics" / "buildstock.csv", index=False)

    def sequential_parallel(**kwargs):
        kw2 = kwargs.copy()
        kw2["n_jobs"] = 1
        return joblib.Parallel(**kw2)

    mocker.patch("buildstockbatch.eagle.shutil.copy2")
    rmtree_mock = mocker.patch("buildstockbatch.eagle.shutil.rmtree")
    mocker.patch("buildstockbatch.eagle.Parallel", sequential_parallel)
    mocker.patch("buildstockbatch.eagle.subprocess")

    mocker.patch.object(EagleBatch, "local_buildstock_dir", results_dir / "local_buildstock_dir")
    mocker.patch.object(EagleBatch, "local_weather_dir", results_dir / "local_weather_dir")
    mocker.patch.object(EagleBatch, "local_output_dir", results_dir)
    mocker.patch.object(
        EagleBatch,
        "local_housing_characteristics_dir",
        results_dir / "local_housing_characteristics_dir",
    )
    mocker.patch.object(EagleBatch, "results_dir", results_dir)
    mocker.patch.object(EagleBatch, "local_scratch", results_dir.parent)

    def make_sim_dir_mock(building_id, upgrade_idx, base_dir, overwrite_existing=False):
        real_upgrade_idx = 0 if upgrade_idx is None else upgrade_idx + 1
        sim_id = f"bldg{building_id:07d}up{real_upgrade_idx:02d}"
        sim_dir = os.path.join(base_dir, f"up{real_upgrade_idx:02d}", f"bldg{building_id:07d}")
        return sim_id, sim_dir

    mocker.patch.object(EagleBatch, "make_sim_dir", make_sim_dir_mock)
    sampler_prop_mock = mocker.patch.object(EagleBatch, "sampler", new_callable=mocker.PropertyMock)
    sampler_mock = mocker.MagicMock()
    sampler_prop_mock.return_value = sampler_mock
    sampler_mock.csv_path = results_dir.parent / "housing_characteristic2" / "buildstock.csv"
    sampler_mock.run_sampling = mocker.MagicMock(return_value="buildstock.csv")

    b = EagleBatch(project_filename)
    b.run_batch(sampling_only=True)  # so the directories can be created
    sampler_mock.run_sampling.assert_called_once()
    b.run_job_batch(1)
    rmtree_mock.assert_any_call(b.local_buildstock_dir)
    rmtree_mock.assert_any_call(b.local_weather_dir)
    rmtree_mock.assert_any_call(b.local_output_dir)
    rmtree_mock.assert_any_call(b.local_housing_characteristics_dir)

    # check results job-json
    refrence_path = pathlib.Path(__file__).resolve().parent / "test_results" / "reference_files"

    refrence_list = json.loads(gzip.open(refrence_path / "results_job1.json.gz", "r").read())

    output_list = json.loads(gzip.open(results_dir / "simulation_output" / "results_job1.json.gz", "r").read())

    refrence_list = [json.dumps(d) for d in refrence_list]
    output_list = [json.dumps(d) for d in output_list]

    assert sorted(refrence_list) == sorted(output_list)

    ts_files = list(refrence_path.glob("**/*.parquet"))

    def compare_ts_parquets(source, dst):
        test_pq = pd.read_parquet(source).reset_index().drop(columns=["index"]).rename(columns=str.lower)
        reference_pq = pd.read_parquet(dst).reset_index().drop(columns=["index"]).rename(columns=str.lower)
        pd.testing.assert_frame_equal(test_pq, reference_pq)

    for file in ts_files:
        results_file = results_dir / "results" / "simulation_output" / "timeseries" / file.parent.name / file.name
        compare_ts_parquets(file, results_file)

    # Check that buildstock.csv was trimmed properly
    local_buildstock_df = read_csv(results_dir / "local_housing_characteristics_dir" / "buildstock.csv", dtype=str)
    unique_buildings = {str(x[0]) for x in job_json["batch"]}
    assert len(unique_buildings) == len(local_buildstock_df)
    assert unique_buildings == set(local_buildstock_df["Building"])


def test_run_building_error_caught(mocker, basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()
    results_dir = pathlib.Path(results_dir)

    job_json = {"job_num": 1, "batch": [(1, 0)], "n_datapoints": 1}
    with open(results_dir / "job001.json", "w") as f:
        json.dump(job_json, f)

    sample_buildstock_csv = pd.DataFrame.from_records([{"Building": i, "Dummy Column": i * i} for i in range(10)])
    os.makedirs(results_dir / "housing_characteristics", exist_ok=True)
    os.makedirs(results_dir / "local_housing_characteristics", exist_ok=True)
    os.makedirs(results_dir / "weather", exist_ok=True)
    sample_buildstock_csv.to_csv(results_dir / "housing_characteristics" / "buildstock.csv", index=False)

    def raise_error(*args, **kwargs):
        raise RuntimeError("A problem happened")

    def sequential_parallel(**kwargs):
        kw2 = kwargs.copy()
        kw2["n_jobs"] = 1
        return joblib.Parallel(**kw2)

    mocker.patch("buildstockbatch.eagle.shutil.copy2")
    mocker.patch("buildstockbatch.eagle.shutil.rmtree")
    mocker.patch("buildstockbatch.eagle.Parallel", sequential_parallel)
    mocker.patch("buildstockbatch.eagle.subprocess")

    mocker.patch.object(EagleBatch, "run_building", raise_error)
    mocker.patch.object(EagleBatch, "local_output_dir", results_dir)
    mocker.patch.object(EagleBatch, "results_dir", results_dir)
    mocker.patch.object(EagleBatch, "local_buildstock_dir", results_dir / "local_buildstock_dir")
    mocker.patch.object(EagleBatch, "local_weather_dir", results_dir / "local_weather_dir")
    mocker.patch.object(
        EagleBatch,
        "local_housing_characteristics_dir",
        results_dir / "local_housing_characteristics_dir",
    )

    b = EagleBatch(project_filename)
    b.run_job_batch(1)

    traceback_file = results_dir / "simulation_output" / "traceback1.out"
    assert traceback_file.exists()

    with open(traceback_file, "r") as f:
        assert f.read().find("RuntimeError") > -1


def test_rerun_failed_jobs(mocker, basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()
    os.makedirs(os.path.join(results_dir, "results_csvs"))
    os.makedirs(os.path.join(results_dir, "parquet"))
    mocker.patch.object(EagleBatch, "weather_dir", None)
    mocker.patch.object(EagleBatch, "results_dir", results_dir)
    process_results_mocker = mocker.patch.object(BuildStockBatchBase, "process_results")
    queue_jobs_mocker = mocker.patch.object(EagleBatch, "queue_jobs", return_value=[42])
    queue_post_processing_mocker = mocker.patch.object(EagleBatch, "queue_post_processing")

    b = EagleBatch(project_filename)

    for job_id in range(1, 6):
        json_filename = os.path.join(b.output_dir, f"job{job_id:03d}.json")
        with open(json_filename, "w") as f:
            json.dump({}, f)
        if job_id == 5:
            continue
        out_filename = os.path.join(b.output_dir, f"job.out-{job_id}")
        with open(out_filename, "w") as f:
            f.write("lots of output\ngoes\nhere\n")
            if job_id % 2 == 0:
                f.write("Traceback")
            else:
                f.write("batch complete")
            f.write("\n")

    failed_array_ids = b.get_failed_job_array_ids()
    assert sorted(failed_array_ids) == [2, 4, 5]

    assert not b.process_results()
    process_results_mocker.assert_not_called()
    process_results_mocker.reset_mock()

    b.rerun_failed_jobs()
    queue_jobs_mocker.assert_called_once_with([2, 4, 5], hipri=False)
    queue_jobs_mocker.reset_mock()
    queue_post_processing_mocker.assert_called_once_with([42], hipri=False)
    queue_post_processing_mocker.reset_mock()
    assert not os.path.exists(os.path.join(results_dir, "results_csvs"))
    assert not os.path.exists(os.path.join(results_dir, "parquet"))

    for job_id in range(1, 6):
        json_filename = os.path.join(b.output_dir, f"job{job_id:03d}.json")
        with open(json_filename, "w") as f:
            json.dump({}, f)
        out_filename = os.path.join(b.output_dir, f"job.out-{job_id}")
        with open(out_filename, "w") as f:
            f.write("lots of output\ngoes\nhere\n")
            f.write("batch complete\n")

    b.process_results()
    process_results_mocker.assert_called_once()

    assert not b.rerun_failed_jobs()
    queue_jobs_mocker.assert_not_called()
    queue_post_processing_mocker.assert_not_called()
