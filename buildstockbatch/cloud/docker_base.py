# -*- coding: utf-8 -*-

"""
buildstockbatch.docker_base
~~~~~~~~~~~~~~~
This is the base class mixed into classes that deploy using a docker container.

:author: Natalie Weires
:license: BSD-3
"""
import collections
import csv
import docker
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pathlib
import random
import shutil
import subprocess
import tarfile
import time

from buildstockbatch import postprocessing
from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.utils import ContainerRuntime, calc_hash_for_file, compress_file, read_csv

logger = logging.getLogger(__name__)


class DockerBatchBase(BuildStockBatchBase):
    """Base class for implementations that run in Docker containers."""

    CONTAINER_RUNTIME = ContainerRuntime.DOCKER
    MAX_JOB_COUNT = 10000

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.docker_client = docker.DockerClient.from_env()
        try:
            self.docker_client.ping()
        except:  # noqa: E722 (allow bare except in this case because error can be a weird non-class Windows API error)
            logger.error("The docker server did not respond, make sure Docker Desktop is started then retry.")
            raise RuntimeError("The docker server did not respond, make sure Docker Desktop is started then retry.")

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)

    @property
    def docker_image(self):
        return "nrel/openstudio:{}".format(self.os_version)

    @property
    def weather_dir(self):
        return self._weather_dir

    def prep_batches(self, tmppath):
        """
        Prepare batches of samples to be uploaded and run in the cloud.

        This will:
            - Perform the sampling
            - Split the samples into (at most) self.batch_array_size batches
            - Collect and package and the required assets, including weather
              files, and write them to tmppath.

        self.weather_dir must exist before calling this method. This is where weather files are stored temporarily.

        :param tmppath:  Path to a temporary directory where files should be collected before uploading.

        :returns: (job_count, unique_epws), where
            job_count: The number of jobs the samples were split into.
            unique_epws: A dictionary mapping from the hash of weather files to a list of filenames
                with that hashed value. Only the first in each list is written to tmppath, so
                this can be used to recreate the other files later.
        """
        # Generate buildstock.csv
        buildstock_csv_filename = self.sampler.run_sampling()

        self._get_weather_files()
        logger.debug("Creating assets tarfile")
        with tarfile.open(tmppath / "assets.tar.gz", "x:gz") as tar_f:
            project_path = pathlib.Path(self.project_dir)
            buildstock_path = pathlib.Path(self.buildstock_dir)
            tar_f.add(buildstock_path / "measures", "measures")
            if os.path.exists(buildstock_path / "resources/hpxml-measures"):
                tar_f.add(
                    buildstock_path / "resources/hpxml-measures",
                    "resources/hpxml-measures",
                )
            tar_f.add(buildstock_path / "resources", "lib/resources")
            tar_f.add(
                project_path / "housing_characteristics",
                "lib/housing_characteristics",
            )

        # Weather files
        weather_path = tmppath / "weather"
        os.makedirs(weather_path)

        # Determine the unique weather files
        epw_filenames = list(filter(lambda x: x.endswith(".epw"), os.listdir(self.weather_dir)))
        logger.debug("Calculating hashes for weather files")
        epw_hashes = Parallel(n_jobs=-1, verbose=9)(
            delayed(calc_hash_for_file)(pathlib.Path(self.weather_dir) / epw_filename) for epw_filename in epw_filenames
        )
        unique_epws = collections.defaultdict(list)
        for epw_filename, epw_hash in zip(epw_filenames, epw_hashes):
            unique_epws[epw_hash].append(epw_filename)

        # Compress unique weather files
        logger.debug("Compressing weather files")
        Parallel(n_jobs=-1, verbose=9)(
            delayed(compress_file)(
                pathlib.Path(self.weather_dir) / x[0],
                str(weather_path / x[0]) + ".gz",
            )
            for x in unique_epws.values()
        )

        logger.debug("Writing project configuration for upload")
        with open(tmppath / "config.json", "wt", encoding="utf-8") as f:
            json.dump(self.cfg, f)

        # Collect simulations to queue
        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        n_sims = n_datapoints * (len(self.cfg.get("upgrades", [])) + 1)
        logger.debug("Total number of simulations = {}".format(n_sims))

        # This is the maximum number of jobs that can be in an array
        if self.batch_array_size <= self.MAX_JOB_COUNT:
            max_array_size = self.batch_array_size
        else:
            max_array_size = self.MAX_JOB_COUNT
        n_sims_per_job = math.ceil(n_sims / max_array_size)
        n_sims_per_job = max(n_sims_per_job, 2)
        logger.debug("Number of simulations per array job = {}".format(n_sims_per_job))

        # Create list of (building ID, upgrade to apply) pairs for all simulations to run.
        baseline_sims = zip(building_ids, itertools.repeat(None))
        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get("upgrades", []))))
        all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        os.makedirs(tmppath / "jobs")

        # Write each batch of simulations to a file.
        logger.info("Queueing jobs")
        for i in itertools.count(0):
            batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
            if not batch:
                break
            job_json_filename = tmppath / "jobs" / "job{:05d}.json".format(i)
            with open(job_json_filename, "w") as f:
                json.dump(
                    {
                        "job_num": i,
                        "n_datapoints": n_datapoints,
                        "batch": batch,
                    },
                    f,
                    indent=4,
                )
        job_count = i
        logger.debug("Job count = {}".format(job_count))

        # Compress job jsons
        jobs_dir = tmppath / "jobs"
        logger.debug("Compressing job jsons using gz")
        tick = time.time()
        with tarfile.open(tmppath / "jobs.tar.gz", "w:gz") as tf:
            tf.add(jobs_dir, arcname="jobs")
        tick = time.time() - tick
        logger.debug("Done compressing job jsons using gz {:.1f} seconds".format(tick))
        shutil.rmtree(jobs_dir)

        os.makedirs(tmppath / "results" / "simulation_output")

        return (job_count, unique_epws)

    @classmethod
    def get_epws_to_download(cls, sim_dir, jobs_d):
        """
        Gets the list of filenames for the weather data required for a single batch of simulations.

        :param sim_dir: Path to the directory where job files are stored
        :param jobs_d: Contents of a single job JSON file; contains the list of buildings to simulate in this job.

        :returns: Set of epw filenames needed for this batch of simulations.
        """
        # Make a lookup of which parameter points to the weather file from options_lookup.tsv
        with open(sim_dir / "lib" / "resources" / "options_lookup.tsv", "r", encoding="utf-8") as f:
            tsv_reader = csv.reader(f, delimiter="\t")
            next(tsv_reader)  # skip headers
            param_name = None
            epws_by_option = {}
            for row in tsv_reader:
                row_has_epw = [x.endswith(".epw") for x in row[2:]]
                if sum(row_has_epw):
                    if row[0] != param_name and param_name is not None:
                        raise RuntimeError(
                            "The epw files are specified in options_lookup.tsv under more than one parameter type: "
                            f"{param_name}, {row[0]}"
                        )
                    epw_filename = row[row_has_epw.index(True) + 2].split("=")[1]
                    param_name = row[0]
                    option_name = row[1]
                    epws_by_option[option_name] = epw_filename

        # Look through the buildstock.csv to find the appropriate location and epw
        epws_to_download = set()
        building_ids = [x[0] for x in jobs_d["batch"]]
        with open(
            sim_dir / "lib" / "housing_characteristics" / "buildstock.csv",
            "r",
            encoding="utf-8",
        ) as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if int(row["Building"]) in building_ids:
                    epws_to_download.add(epws_by_option[row[param_name]])

        return epws_to_download

    @classmethod
    def run_simulations(cls, cfg, job_id, jobs_d, sim_dir, fs, bucket, prefix):
        """
        Run one batch of simulations.

        Runs the simulations, writes outputs to the provided storage bucket, and cleans up intermediate files.

        :param cfg: Project config contents.
        :param job_id: Index of this job.
        :param jobs_d: Contents of a single job JSON file; contains the list of buildings to simulate in this job.
        :param sim_dir: Path to the (local) directory where job files are stored.
        :param fs: Filesystem to use when writing outputs to storage bucket
        :param bucket: Name of the storage bucket to upload results to.
        :param prefix: File prefix to use when writing to storage bucket.
        """
        local_fs = LocalFileSystem()
        reporting_measures = cls.get_reporting_measures(cfg)
        dpouts = []
        simulation_output_tar_filename = sim_dir.parent / "simulation_outputs.tar.gz"
        asset_dirs = os.listdir(sim_dir)
        ts_output_dir = (f"{bucket}/{prefix}/results/simulation_output/timeseries",)

        with tarfile.open(str(simulation_output_tar_filename), "w:gz") as simout_tar:
            for building_id, upgrade_idx in jobs_d["batch"]:
                upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1
                sim_id = f"bldg{building_id:07d}up{upgrade_id:02d}"

                # Create OSW
                osw = cls.create_osw(cfg, jobs_d["n_datapoints"], sim_id, building_id, upgrade_idx)
                with open(os.path.join(sim_dir, "in.osw"), "w") as f:
                    json.dump(osw, f, indent=4)

                # Run Simulation
                with open(sim_dir / "os_stdout.log", "w") as f_out:
                    try:
                        logger.debug("Running {}".format(sim_id))
                        subprocess.run(
                            ["openstudio", "run", "-w", "in.osw"],
                            check=True,
                            stdout=f_out,
                            stderr=subprocess.STDOUT,
                            cwd=str(sim_dir),
                        )
                    except subprocess.CalledProcessError:
                        logger.debug(f"Simulation failed: see {sim_id}/os_stdout.log")

                # Clean Up simulation directory
                cls.cleanup_sim_dir(
                    sim_dir,
                    fs,
                    ts_output_dir,
                    upgrade_id,
                    building_id,
                )

                # Read data_point_out.json
                dpout = postprocessing.read_simulation_outputs(
                    local_fs, reporting_measures, str(sim_dir), upgrade_id, building_id
                )
                dpouts.append(dpout)

                # Add the rest of the simulation outputs to the tar archive
                logger.info("Archiving simulation outputs")
                for dirpath, dirnames, filenames in os.walk(sim_dir):
                    if dirpath == str(sim_dir):
                        for dirname in set(dirnames).intersection(asset_dirs):
                            dirnames.remove(dirname)
                    for filename in filenames:
                        abspath = os.path.join(dirpath, filename)
                        relpath = os.path.relpath(abspath, sim_dir)
                        simout_tar.add(abspath, os.path.join(sim_id, relpath))

                # Clear directory for next simulation
                logger.debug("Clearing out simulation directory")
                for item in set(os.listdir(sim_dir)).difference(asset_dirs):
                    if os.path.isdir(item):
                        shutil.rmtree(item)
                    elif os.path.isfile(item):
                        os.remove(item)

        # Upload simulation outputs tarfile to s3
        fs.put(
            str(simulation_output_tar_filename),
            f"{bucket}/{prefix}/results/simulation_output/simulations_job{job_id}.tar.gz",
        )

        # Upload aggregated dpouts as a json file
        with fs.open(
            f"{bucket}/{prefix}/results/simulation_output/results_job{job_id}.json.gz",
            "wb",
        ) as f1:
            with gzip.open(f1, "wt", encoding="utf-8") as f2:
                json.dump(dpouts, f2)

        # Remove files (it helps docker if we don't leave a bunch of files laying around)
        os.remove(simulation_output_tar_filename)
        for item in os.listdir(sim_dir):
            if os.path.isdir(item):
                shutil.rmtree(item)
            elif os.path.isfile(item):
                os.remove(item)
