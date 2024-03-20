"""Tests for the DockerBatchBase class."""

from fsspec.implementations.local import LocalFileSystem
import gzip
import json
import os
import pathlib
import pytest
import shutil
import tarfile
import tempfile
from unittest.mock import MagicMock, PropertyMock

from buildstockbatch.cloud import docker_base
from buildstockbatch.cloud.docker_base import DockerBatchBase
from buildstockbatch.test.shared_testing_stuff import docker_available
from buildstockbatch.utils import get_project_configuration

here = os.path.dirname(os.path.abspath(__file__))
resources_dir = os.path.join(here, "test_inputs", "test_openstudio_buildstock", "resources")


@docker_available
@pytest.mark.parametrize("skip_baseline_sims,expected", [(False, 10), (True, 5)])
def test_skip_baseline_sims(basic_residential_project_file, mocker, skip_baseline_sims, expected):
    """Test "skip_sims" baseline configuration's effect on ``n_sims``"""
    update_args = {"baseline": {"skip_sims": True}} if skip_baseline_sims else {}
    project_filename, results_dir = basic_residential_project_file(update_args)

    mocker.patch.object(DockerBatchBase, "results_dir", results_dir)
    sampler_property_mock = mocker.patch.object(DockerBatchBase, "sampler", new_callable=PropertyMock)
    sampler_mock = mocker.MagicMock()
    sampler_property_mock.return_value = sampler_mock
    # Hard-coded sampling output includes 5 buildings.
    sampler_mock.run_sampling = MagicMock(return_value=os.path.join(resources_dir, "buildstock_good.csv"))

    dbb = DockerBatchBase(project_filename)
    dbb.batch_array_size = 3
    DockerBatchBase.validate_project = MagicMock(return_value=True)

    with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir:
        _, batch_info = dbb._run_batch_prep(pathlib.Path(tmpdir))
        assert batch_info.n_sims == expected


@docker_available
def test_run_batch_prep(basic_residential_project_file, mocker):
    """Test that samples are created and bundled into batches correctly."""
    project_filename, results_dir = basic_residential_project_file()

    mocker.patch.object(DockerBatchBase, "results_dir", results_dir)
    sampler_property_mock = mocker.patch.object(DockerBatchBase, "sampler", new_callable=PropertyMock)
    sampler_mock = mocker.MagicMock()
    sampler_property_mock.return_value = sampler_mock
    # Hard-coded sampling output includes 5 buildings.
    sampler_mock.run_sampling = MagicMock(return_value=os.path.join(resources_dir, "buildstock_good.csv"))

    dbb = DockerBatchBase(project_filename)
    dbb.batch_array_size = 3
    DockerBatchBase.validate_project = MagicMock(return_value=True)

    with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir:
        tmppath = pathlib.Path(tmpdir)
        epws_to_copy, batch_info = dbb._run_batch_prep(tmppath)
        sampler_mock.run_sampling.assert_called_once()

        # There are three weather files...
        #   * "G2500210.epw" is unique; check for it (gz'd) in tmppath
        #   * "G2601210.epw" and "G2601390.epw" are dupes. One should be in
        #     tmppath; one should be copied to the other according to ``epws_to_copy``
        assert os.path.isfile(tmppath / "weather" / "G2500210.epw.gz")
        assert os.path.isfile(tmppath / "weather" / "G2601210.epw.gz") or os.path.isfile(
            tmppath / "weather" / "G2601390.epw.gz"
        )
        src, dest = epws_to_copy[0]
        assert src in ("G2601210.epw.gz", "G2601390.epw.gz")
        assert dest in ("G2601210.epw.gz", "G2601390.epw.gz")
        assert src != dest

        # Three job files should be created, with 10 total simulations, split
        # into batches of 4, 4, and 2 simulations.
        assert batch_info.n_sims == 10
        assert batch_info.n_sims_per_job == 4
        assert batch_info.job_count == 3
        jobs_file_path = tmppath / "jobs.tar.gz"
        with tarfile.open(jobs_file_path, "r") as tar_f:
            all_job_files = ["jobs", "jobs/job00000.json", "jobs/job00001.json", "jobs/job00002.json"]
            assert tar_f.getnames() == all_job_files
            simulations = []
            for filename in all_job_files[1:]:
                job = json.load(tar_f.extractfile(filename))
                assert filename == f"jobs/job{job['job_num']:05d}.json"
                assert job["n_datapoints"] == 5  # Total number of buildings
                assert len(job["batch"]) in (2, 4)  # Number of simulations in this batch
                simulations.extend(job["batch"])

            # Check that all 10 expected simulations are present
            assert len(simulations) == 10
            for building in range(1, 6):
                # Building baseline
                assert [building, None] in simulations
                # Building with upgrade 0
                assert [building, 0] in simulations


def test_get_epws_to_download():
    resources_dir_path = pathlib.Path(resources_dir)
    options_file = resources_dir_path / "options_lookup.tsv"
    buildstock_file = resources_dir_path / "buildstock_good.csv"

    with tempfile.TemporaryDirectory(prefix="bsb_") as sim_dir_str:
        sim_dir = pathlib.Path(sim_dir_str)
        os.makedirs(sim_dir / "lib" / "resources")
        os.makedirs(sim_dir / "lib" / "housing_characteristics")
        shutil.copy(options_file, sim_dir / "lib" / "resources")
        shutil.copy(buildstock_file, sim_dir / "lib" / "housing_characteristics" / "buildstock.csv")

        jobs_d = {
            "job_num": 0,
            "n_datapoints": 10,
            "batch": [
                [1, None],
                [5, None],
            ],
        }

        epws = docker_base.determine_epws_needed_for_job(sim_dir, jobs_d)
        assert epws == {"weather/G2500210.epw", "weather/G2601390.epw"}


def test_run_simulations(basic_residential_project_file):
    """
    Test running a single batch of simulation.

    This doesn't provide all the necessary inputs for the simulations to succeed, but it confirms that they are
    attempted, that the output files are produced, and that intermediate files are cleaned up.
    """
    jobs_d = {
        "job_num": 0,
        "n_datapoints": 10,
        "batch": [
            [1, None],
            [5, None],
        ],
    }
    fs = LocalFileSystem()
    project_filename, results_dir = basic_residential_project_file()
    cfg = get_project_configuration(project_filename)

    with tempfile.TemporaryDirectory(prefix="bsb_") as temp_dir_str:
        temp_path = pathlib.Path(temp_dir_str)
        sim_dir = temp_path / "simdata" / "openstudio"
        os.makedirs(sim_dir)
        # sim_dir is also the working directory (defined in the nrel/openstudio
        # Dockerfile), which some file operations depend on.
        old_cwd = os.getcwd()
        os.chdir(sim_dir)
        bucket = temp_path / "bucket"
        os.makedirs(bucket / "test_prefix" / "results" / "simulation_output")

        DockerBatchBase.run_simulations(cfg, 0, jobs_d, sim_dir, fs, f"{bucket}/test_prefix")

        output_dir = bucket / "test_prefix" / "results" / "simulation_output"
        assert sorted(os.listdir(output_dir)) == ["results_job0.json.gz", "simulations_job0.tar.gz"]

        # Check that buildings 1 and 5 (specified in jobs_d) are in the results
        with gzip.open(output_dir / "results_job0.json.gz", "r") as f:
            results = json.load(f)
        assert len(results) == 2
        for building in results:
            assert building["building_id"] in (1, 5)

        # Check that files were cleaned up correctly
        assert not os.listdir(sim_dir)
        os.chdir(old_cwd)


def test_find_missing_tasks(basic_residential_project_file, mocker):
    project_filename, results_dir = basic_residential_project_file()
    results_dir = os.path.join(results_dir, "results")
    mocker.patch.object(DockerBatchBase, "results_dir", results_dir)
    dbb = DockerBatchBase(project_filename)

    existing_results = [1, 3, 5]
    missing_results = [0, 2, 4, 6]
    os.makedirs(os.path.join(results_dir, "simulation_output"))
    for res in existing_results:
        # Create empty results files
        with open(os.path.join(results_dir, f"simulation_output/results_job{res}.json.gz"), "w"):
            pass

    assert dbb.find_missing_tasks(7) == len(missing_results)

    with open(os.path.join(results_dir, "missing_tasks.txt"), "r") as f:
        assert [int(t) for t in f.readlines()] == missing_results
