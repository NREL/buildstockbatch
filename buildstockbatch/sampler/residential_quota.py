"""
buildstockbatch.sampler.residential_quota
~~~~~~~~~~~~~~~
This object contains the code required for generating the set of simulations to execute

:author: Noel Merket, Ry Horsey
:copyright: (c) 2020 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import docker
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import time

from .base import BuildStockSampler
from .downselect import DownselectSamplerBase
from buildstockbatch.exc import ValidationError

logger = logging.getLogger(__name__)


class ResidentialQuotaSampler(BuildStockSampler):
    def __init__(self, parent, n_datapoints):
        """Residential Quota Sampler

        :param parent: BuildStockBatchBase object
        :type parent: BuildStockBatchBase (or subclass)
        :param n_datapoints: number of datapoints to sample
        :type n_datapoints: int
        """
        super().__init__(parent)
        self.validate_args(self.parent().project_filename, n_datapoints=n_datapoints)
        self.n_datapoints = n_datapoints

    @classmethod
    def validate_args(cls, project_filename, **kw):
        expected_args = set(["n_datapoints"])
        for k, v in kw.items():
            expected_args.discard(k)
            if k == "n_datapoints":
                if not isinstance(v, int):
                    raise ValidationError("n_datapoints needs to be an integer")
                if v <= 0:
                    raise ValidationError("n_datapoints need to be >= 1")
            else:
                raise ValidationError(f"Unknown argument for sampler: {k}")
        if len(expected_args) > 0:
            raise ValidationError("The following sampler arguments are required: " + ", ".join(expected_args))
        return True

    def _run_sampling_docker(self):
        docker_client = docker.DockerClient.from_env()
        tick = time.time()
        extra_kws = {}
        if sys.platform.startswith("linux"):
            extra_kws["user"] = f"{os.getuid()}:{os.getgid()}"
        container_output = docker_client.containers.run(
            self.parent().docker_image,
            [
                "ruby",
                "resources/run_sampling.rb",
                "-p",
                self.cfg["project_directory"],
                "-n",
                str(self.n_datapoints),
                "-o",
                "buildstock.csv",
            ],
            remove=True,
            volumes={self.buildstock_dir: {"bind": "/var/simdata/openstudio", "mode": "rw"}},
            name="buildstock_sampling",
            **extra_kws,
        )
        tick = time.time() - tick
        for line in container_output.decode("utf-8").split("\n"):
            logger.debug(line)
        logger.debug("Sampling took {:.1f} seconds".format(tick))
        destination_filename = self.csv_path
        if os.path.exists(destination_filename):
            os.remove(destination_filename)
        shutil.move(
            os.path.join(self.buildstock_dir, "resources", "buildstock.csv"),
            destination_filename,
        )
        return destination_filename

    def _run_sampling_apptainer(self):
        args = [
            "apptainer",
            "exec",
            "--contain",
            "--home",
            "{}:/buildstock".format(self.buildstock_dir),
            "--bind",
            "{}:/outbind".format(os.path.dirname(self.csv_path)),
            self.parent().apptainer_image,
            "ruby",
            "resources/run_sampling.rb",
            "-p",
            self.cfg["project_directory"],
            "-n",
            str(self.n_datapoints),
            "-o",
            "../../outbind/{}".format(os.path.basename(self.csv_path)),
        ]
        logger.debug(f"Starting apptainer sampling with command: {' '.join(args)}")
        subprocess.run(args, check=True, env=os.environ, cwd=self.parent().output_dir)
        logger.debug("Apptainer sampling completed.")
        return self.csv_path

    def _run_sampling_local_openstudio(self):
        subprocess.run(
            [
                self.parent().openstudio_exe(),
                str(pathlib.Path("resources", "run_sampling.rb")),
                "-p",
                self.cfg["project_directory"],
                "-n",
                str(self.n_datapoints),
                "-o",
                "buildstock.csv",
            ],
            cwd=self.buildstock_dir,
            check=True,
        )
        destination_filename = pathlib.Path(self.csv_path)
        if destination_filename.exists():
            os.remove(destination_filename)
        shutil.move(
            pathlib.Path(self.buildstock_dir, "resources", "buildstock.csv"),
            destination_filename,
        )
        return destination_filename


class ResidentialQuotaDownselectSampler(DownselectSamplerBase):
    SUB_SAMPLER_CLASS = ResidentialQuotaSampler
