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
from dataclasses import dataclass
import docker
from fsspec.implementations.local import LocalFileSystem
import glob
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pandas as pd
import pathlib
import random
import re
import shutil
import subprocess
import tarfile
import tempfile
import time

from buildstockbatch import postprocessing
from buildstockbatch.base import BuildStockBatchBase, ValidationError
from buildstockbatch.utils import ContainerRuntime, calc_hash_for_file, compress_file, read_csv

logger = logging.getLogger(__name__)


def determine_epws_needed_for_job(sim_dir, jobs_d):
    """
    Gets the list of filenames for the weather data required for a job of simulations.

    :param sim_dir: Path to the directory where job files are stored
    :param jobs_d: Contents of a single job JSON file; contains the list of buildings to simulate in this job.

    :returns: Set of epw filenames needed for this job of simulations.
    """
    # Fetch the mapping for building to weather file from options_lookup.tsv
    epws_by_option, param_name = _epws_by_option(sim_dir / "lib" / "resources" / "options_lookup.tsv")

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


def _epws_by_option(options_lookup_path):
    epws_by_option = {}
    with open(options_lookup_path, "r", encoding="utf-8") as f:
        tsv_reader = csv.reader(f, delimiter="\t")
        next(tsv_reader)  # skip headers
        param_name = None
        for row in tsv_reader:
            row_has_epw = [x.endswith(".epw") for x in row[2:]]
            if sum(row_has_epw):
                if row[0] != param_name and param_name is not None:
                    raise RuntimeError(
                        "The epw files are specified in options_lookup.tsv under more than one parameter "
                        f"type: {param_name}, {row[0]}"
                    )  # noqa: E501
                epw_filename = row[row_has_epw.index(True) + 2].split("=")[1]
                param_name = row[0]
                option_name = row[1]
                epws_by_option[option_name] = epw_filename
    return (epws_by_option, param_name)


class DockerBatchBase(BuildStockBatchBase):
    """Base class for implementations that run in Docker containers."""

    @dataclass
    class BatchInfo:
        """Information about the Batch jobs to be run."""

        # The total number of simulations that will be run.
        n_sims: int

        # The total number of simulations that each job will run.
        n_sims_per_job: int

        # The number of jobs the samples were split into.
        job_count: int

    CONTAINER_RUNTIME = ContainerRuntime.DOCKER
    MAX_JOB_COUNT = 10000

    def __init__(self, project_filename, missing_only=False):
        """
        :param missing_only: If true, use asset files from a previous job and only run the simulations
            that don't already have successful results from a previous run. Support must be added in subclasses.
        """
        super().__init__(project_filename)
        self.missing_only = missing_only

        self.docker_client = docker.DockerClient.from_env()
        try:
            self.docker_client.ping()
        except:  # noqa: E722 (allow bare except in this case because error can be a weird non-class Windows API error)
            logger.error("The docker server did not respond, make sure Docker Desktop is started then retry.")
            raise RuntimeError("The docker server did not respond, make sure Docker Desktop is started then retry.")

    def get_fs(self):
        return LocalFileSystem()

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)

    @property
    def docker_image(self):
        return "nrel/openstudio:{}".format(self.os_version)

    @property
    def weather_dir(self):
        return self._weather_dir

    def upload_batch_files_to_cloud(self, tmppath):
        """Upload all files in ``tmppath`` to the cloud (where they will be used by the batch
        jobs).
        """
        raise NotImplementedError

    def copy_files_at_cloud(self, files_to_copy):
        """Copy files from-cloud-to-cloud storage. This is used to avoid using bandwidth to upload
        duplicate files.

        :param files_to_copy: a dict where the key is a file on the cloud to copy, and the value is
            the filename to copy the source file to. Both are relative to the ``tmppath`` used in
            ``_run_batch_prep()`` (so the implementation should prepend the bucket name and prefix
            where they were uploaded to by ``upload_batch_files_to_cloud``).
        """
        raise NotImplementedError

    def start_batch_job(self, batch_info):
        """Create and start the Batch job on the cloud.

        Files used by the batch job will have been prepared and uploaded (by
        :func:`DockerBase.run_batch`, which is what runs this).

        :param batch_info: A :class:`DockerBatchBase.BatchInfo` containing information about the job
        """
        raise NotImplementedError

    def run_batch(self):
        """Prepare and start a Batch job on the cloud to run simulations.

        This does all the cloud-agnostic prep (such as preparing weather files, assets, and job
        definition), delegating to the implementations to upload those files to the cloud (using
        (:func:`upload_batch_files_to_cloud` and :func:`copy_files_at_cloud`), and then calls the
        implementation's :func:`start_batch_job` to actually create and start the batch job.
        """
        with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir:
            tmppath = pathlib.Path(tmpdir)
            epws_to_copy, batch_info = self._run_batch_prep(tmppath)

            # If we're rerunning failed tasks from a previous job, DO NOT overwrite the job files.
            # That would assign a new random set of buildings to each task, making the rerun useless.
            if not self.missing_only:
                # Copy all the files to cloud storage
                logger.info("Uploading files for batch...")
                self.upload_batch_files_to_cloud(tmppath)

                logger.info("Copying duplicate weather files...")
                self.copy_files_at_cloud(epws_to_copy)

        self.start_batch_job(batch_info)

    def _run_batch_prep(self, tmppath):
        """Do preparation for running the Batch jobs on the cloud, including producing and uploading
        files to the cloud that the batch jobs will use.

        This includes:
            - Sampling, and splitting the samples into (at most) ``self.batch_array_size`` batches,
              and bundling other assets needed for running simulations (:func:`_prep_jobs_for_batch`)
            - Weather files (:func:`_prep_weather_files_for_batch`)

        Those functions place their files to be uploaded into ``tmppath``, and then this will upload
        them to the cloud using (:func:`upload_batch_files_to_cloud`).

        Duplicate weather files will have been excluded from ``tmppath``, and this will use
        (:func:`copy_files_at_cloud`) to copy those files from-cloud-to-cloud (instead of uploading
        them).

        ``self.weather_dir`` must exist before calling this method. This is where weather files are
        stored temporarily.

        This takes ``tmppath`` (rather than managing itself) for testability (so test can manage and
        inspect the contents of the tmppath).

        :returns: DockerBatchBase.BatchInfo
        """

        if not self.missing_only:
            # Project configuration
            logger.info("Writing project configuration for upload...")
            with open(tmppath / "config.json", "wt", encoding="utf-8") as f:
                json.dump(self.cfg, f)

        # Collect simulations to queue (along with the EPWs those sims need)
        logger.info("Preparing simulation batch jobs...")
        batch_info, epws_needed = self._prep_jobs_for_batch(tmppath)

        if self.missing_only:
            epws_to_copy = None
        else:
            # Weather files
            logger.info("Prepping weather files...")
            epws_to_copy = self._prep_weather_files_for_batch(tmppath, epws_needed)

        return (epws_to_copy, batch_info)

    def _prep_weather_files_for_batch(self, tmppath, epws_needed_set):
        """Prepare the weather files (EPWs) needed by the batch.

        1. Downloads, if necessary, and extracts weather files to ``self._weather_dir``.
        2. Ensures that all EPWs needed by the batch are present.
        3. Identifies weather files thare are duplicates to avoid redundant compression work and
           bytes uploaded to the cloud.
            * Puts unique files in the ``tmppath`` (in the 'weather' subdir) which will get uploaded
              to the cloud along with other batch files.
            * Returns a list duplicates, which allows them to be quickly recreated on the cloud via
              copying from-cloud-to-cloud.

        :param tmppath: Unique weather files (compressed) will be copied into a 'weather' subdir
            of this path.
        :param epws_needed_set: A set of weather filenames needed by the batch.
        :returns: an array of tuples where the first value is the filename of a file that will be
            uploaded to cloud storage (because it's in the ``tmppath``), and the second value is the
            filename that the first should be copied to.
            For example, ``[("G2601210.epw.gz", "G2601390.epw.gz")]``.
        """
        with tempfile.TemporaryDirectory(prefix="bsb_") as tmp_weather_in_dir:
            self._weather_dir = tmp_weather_in_dir

            # Downloads, if necessary, and extracts weather files to ``self._weather_dir``
            self._get_weather_files()

            # Ensure all needed EPWs are present
            logger.info("Ensuring all needed weather files are present...")
            epw_files = set(map(lambda x: x.split("/")[-1], glob.glob(f"{self.weather_dir}/*.epw")))
            missing_epws = set()
            for needed_epw in epws_needed_set:
                if needed_epw not in epw_files:
                    missing_epws.add(needed_epw)
            if missing_epws:
                raise ValidationError(
                    "Not all weather files referenced by the sampled buildstock are available. "
                    f"{len(missing_epws):,} missing EPWs: {missing_epws}."
                )
            logger.debug("...all needed weather files are present.")

            # Determine the unique weather files
            logger.info("Calculating hashes for weather files")
            epw_filenames = list(epws_needed_set)
            epw_hashes = Parallel(n_jobs=-1, verbose=9)(
                delayed(calc_hash_for_file)(pathlib.Path(self.weather_dir) / epw_filename)
                for epw_filename in epw_filenames
            )
            # keep track of unique EPWs that may have dupes, and to compress and upload to cloud
            unique_epws = collections.defaultdict(list)
            # keep track of duplicates of the unique EPWs to copy (from cloud-to-cloud)
            epws_to_copy = []
            for epw_filename, epw_hash in zip(epw_filenames, epw_hashes):
                if bool(unique_epws[epw_hash]):
                    # not the first file with this hash (it's a duplicate). add to ``epws_to_copy``
                    epws_to_copy.append((unique_epws[epw_hash][0] + ".gz", epw_filename + ".gz"))
                unique_epws[epw_hash].append(epw_filename)

            # Compress unique weather files and save to ``tmp_weather_out_path``, which will get
            # uploaded to cloud storage
            logger.info("Compressing unique weather files")
            tmp_weather_out_path = tmppath / "weather"
            os.makedirs(tmp_weather_out_path)
            Parallel(n_jobs=-1, verbose=9)(
                delayed(compress_file)(
                    pathlib.Path(self.weather_dir) / x[0],
                    str(tmp_weather_out_path / x[0]) + ".gz",
                )
                for x in unique_epws.values()
            )

            # Calculate and print savings of duplicate files
            upload_bytes = 0
            dupe_count = 0
            dupe_bytes = 0
            for epws in unique_epws.values():
                count = len(epws)
                bytes = os.path.getsize(str(tmp_weather_out_path / epws[0]) + ".gz")
                upload_bytes += bytes
                if count > 1:
                    dupe_count += count - 1
                    dupe_bytes += bytes * (count - 1)
            logger.info(
                f"Weather files: {len(epws_needed_set):,}/{len(epw_files):,} referenced; "
                f"{len(unique_epws):,} unique ({(upload_bytes / 1024 / 1024):,.1f} MiB to upload), "
                f"{dupe_count:,} duplicates ({(dupe_bytes / 1024 / 1024):,.1f} MiB saved from uploading)"
            )
            return epws_to_copy

    def _prep_jobs_for_batch(self, tmppath):
        """Splits simulations into batches, and prepares asset files needed to run them."""
        # Run sampling - generates buildstock.csv
        logger.debug("Running sampling....")
        buildstock_csv_filename = self.sampler.run_sampling()

        logger.debug("Validating sampled buildstock...")
        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        if self.skip_baseline_sims:
            n_sims = n_datapoints * len(self.cfg.get("upgrades", []))
        else:
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
        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get("upgrades", []))))
        if not self.skip_baseline_sims:
            baseline_sims = zip(building_ids, itertools.repeat(None))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        else:
            all_sims = list(upgrade_sims)
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        os.makedirs(tmppath / "jobs")

        # Ensure all weather files are available
        logger.debug("Determining which weather files are needed...")
        epws_needed = self._determine_epws_needed_for_batch(df)

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
        batch_info = DockerBatchBase.BatchInfo(n_sims=n_sims, n_sims_per_job=n_sims_per_job, job_count=job_count)
        if self.missing_only:
            return batch_info, epws_needed

        # Compress job jsons
        jobs_dir = tmppath / "jobs"
        logger.debug("Compressing job jsons using gz")
        tick = time.time()
        with tarfile.open(tmppath / "jobs.tar.gz", "w:gz") as tf:
            tf.add(jobs_dir, arcname="jobs")
        tick = time.time() - tick
        logger.debug("Done compressing job jsons using gz {:.1f} seconds".format(tick))
        shutil.rmtree(jobs_dir)

        # Bundle together assets used when running OpenStudio simulations.
        # Note: The housing_characteristics directory includes buildstock.csv
        # generated by `run_sampling`.
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

        return batch_info, epws_needed

    def _determine_epws_needed_for_batch(self, buildstock_df):
        """
        Gets the list of EPW filenames required for a batch of simulations.

        :param buildstock_df: DataFrame of the buildstock batch being simulated.

        :returns: Set of EPW filenames needed for this batch of simulations.
        """
        # Fetch the mapping for building to weather file from options_lookup.tsv
        epws_by_option, param_name = _epws_by_option(
            pathlib.Path(self.buildstock_dir) / "resources" / "options_lookup.tsv"
        )

        # Iterate over all values in the `param_name` column and collect the referenced EPWs
        epws_needed = set()
        for lookup_value in buildstock_df[param_name]:
            if not lookup_value:
                raise ValidationError(
                    f"Encountered a row in buildstock.csv with an empty value in column: {param_name}"
                )

            epw_path = epws_by_option[lookup_value]
            if not epw_path:
                raise ValidationError(f"Did not find an EPW for '{lookup_value}'")

            # Add just the filename (without relative path)
            epws_needed.add(epw_path.split("/")[-1])

        logger.debug(f"Unique EPWs needed for this buildstock: {len(epws_needed):,}")
        return epws_needed

    @classmethod
    def run_simulations(cls, cfg, job_id, jobs_d, sim_dir, fs, output_path):
        """
        Run one batch of simulations.

        Runs the simulations, writes outputs to the provided storage bucket, and cleans up intermediate files.

        :param cfg: Project config contents.
        :param job_id: Index of this job.
        :param jobs_d: Contents of a single job JSON file; contains the list of buildings to simulate in this job.
        :param sim_dir: Path to the (local) directory where job files are stored.
        :param fs: Filesystem to use when writing outputs to storage bucket
        :param output_path: File path (typically `bucket/prefix`) to write outputs to.
        """
        local_fs = LocalFileSystem()
        reporting_measures = cls.get_reporting_measures(cfg)
        dpouts = []
        simulation_output_tar_filename = sim_dir.parent / "simulation_outputs.tar.gz"
        asset_dirs = os.listdir(sim_dir)
        ts_output_dir = f"{output_path}/results/simulation_output/timeseries"

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

        # Upload simulation outputs tarfile to bucket
        fs.put(
            str(simulation_output_tar_filename),
            f"{output_path}/results/simulation_output/simulations_job{job_id}.tar.gz",
        )

        # Upload aggregated dpouts as a json file
        with fs.open(
            f"{output_path}/results/simulation_output/results_job{job_id}.json.gz",
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

    def find_missing_tasks(self, expected):
        """Creates a file with a list of task numbers that are missing results.

        This only checks for results_job[ID].json.gz files in the results directory.

        :param expected: Number of result files expected.
        :returns: The number of files that were missing.
        """
        fs = self.get_fs()
        done_tasks = set()

        for f in fs.ls(f"{self.results_dir}/simulation_output/"):
            if m := re.match(".*results_job(\\d*).json.gz$", f):
                done_tasks.add(int(m.group(1)))

        missing_tasks = []
        with fs.open(f"{self.results_dir}/missing_tasks.txt", "w") as f:
            for task_id in range(expected):
                if task_id not in done_tasks:
                    f.write(f"{task_id}\n")
                    missing_tasks.append(str(task_id))

        logger.info(f"Found missing tasks: {', '.join(missing_tasks)}")

        return len(missing_tasks)

    def log_summary(self):
        """
        Log a summary of how many simulations succeeded, failed, or ended with other statuses.

        Uses the `completed_status` column of the files in results_csvs/results_*.csv.gz.
        """
        fs = self.get_fs()
        # Summary of simulation statuses across all upgrades
        status_summary = {}
        total_counts = collections.defaultdict(int)

        results_csv_dir = f"{self.results_dir}/results_csvs/"
        try:
            results_files = fs.ls(results_csv_dir)
        except FileNotFoundError:
            logger.info(f"No results CSV files found at {results_csv_dir}")
            return

        for result in results_files:
            upgrade_id = result.split(".")[0][-2:]
            with fs.open(result) as f:
                with gzip.open(f) as gf:
                    df = pd.read_csv(gf, usecols=["completed_status"])
            # Dict mapping from status (e.g. "Success") to count
            statuses = df.groupby("completed_status").size().to_dict()
            status_summary[upgrade_id] = statuses
            for status, count in statuses.items():
                total_counts[status] += count

        # Always include these statuses and show them first
        always_use = ["Success", "Fail"]
        all_statuses = always_use + list(total_counts.keys() - set(always_use))
        s = "Final status of all simulations:"
        for upgrade, counts in status_summary.items():
            if upgrade == "00":
                s += "\nBaseline     "
            else:
                s += f"\nUpgrade {upgrade}   "
            for status in all_statuses:
                s += f"{status}: {counts.get(status, 0):<7d}  "

        s += "\n\nTotal        "
        for status in all_statuses:
            s += f"{status}: {total_counts.get(status, 0):<7d}  "
        s += "\n"

        for upgrade in postprocessing.get_upgrade_list(self.cfg):
            if f"{upgrade:02d}" not in status_summary:
                s += f"\nNo results found for Upgrade {upgrade}"
        logger.info(s)
