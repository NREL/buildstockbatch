"""Tests for the base class in docker_base.py. """
import json
import os
import pathlib
import tarfile
import tempfile
from unittest.mock import MagicMock, PropertyMock

from buildstockbatch.cloud.docker_base import DockerBatchBase

here = os.path.dirname(os.path.abspath(__file__))
resources_dir = os.path.join(here, "test_inputs", "test_openstudio_buildstock", "resources")


def test_prep_batches(basic_residential_project_file, mocker):
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

    with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir, tempfile.TemporaryDirectory(
        prefix="bsb_"
    ) as tmp_weather_dir:
        dbb._weather_dir = tmp_weather_dir
        tmppath = pathlib.Path(tmpdir)

        job_count, unique_epws = dbb.prep_batches(tmppath)
        sampler_mock.run_sampling.assert_called_once()

        # Of the three test weather files, two are identical
        assert sorted(unique_epws.values()) == [
            ["G2500210.epw"],
            ["G2601210.epw", "G2601390.epw"],
        ]

        # Three job files should be created, with 10 total simulations
        assert job_count == 3
        jobs_file_path = tmppath / "jobs.tar.gz"
        with tarfile.open(jobs_file_path, "r") as tar_f:
            assert tar_f.getnames() == ["jobs", "jobs/job00000.json", "jobs/job00001.json", "jobs/job00002.json"]
            job = json.load(tar_f.extractfile("jobs/job00000.json"))
            assert job["job_num"] == 0
            assert job["n_datapoints"] == 5  # Total number of buildings
            assert len(job["batch"]) == 4  # Number of simulations in this batch
