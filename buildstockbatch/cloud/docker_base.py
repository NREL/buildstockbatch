# -*- coding: utf-8 -*-

"""
buildstockbatch.docker_base
~~~~~~~~~~~~~~~
This is the base class mixed into classes that deploy using a docker container.

:author: Natalie Weires
:license: BSD-3
"""
import collections
import docker
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pathlib
import random
import shutil
import tarfile
import time

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
            - Split the samples into self.batch_array_size batches
            - Collect and package and the required assets, including weather files

        :param tmppath:  Path to a temporary directory where files should be collected before uploading.

        Also depends on self.weather_dir existing.

        :returns: (job_count, unique_epws), where
            job_count: The number of jobs the samples were split into.
            unique_epws: A dictionary mapping from the hash of weather files to a list of filenames
                with that hashed value. Only one copy of each unique file is written to tmppath, so
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
