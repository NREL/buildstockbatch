# -*- coding: utf-8 -*-

"""
buildstockbatch.localdocker
~~~~~~~~~~~~~~~
This object contains the code required for execution of local batch simulations

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client, LocalCluster
import datetime as dt
import docker
import functools
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
import time

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch import postprocessing
from buildstockbatch.utils import log_error_details, ContainerRuntime, read_csv
from buildstockbatch.__version__ import __version__ as bsb_version

logger = logging.getLogger(__name__)


class LocalBatch(BuildStockBatchBase):
    CONTAINER_RUNTIME = ContainerRuntime.LOCAL_OPENSTUDIO

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self._weather_dir = None

        # Create simulation_output dir
        sim_out_ts_dir = os.path.join(self.results_dir, "simulation_output", "timeseries")
        os.makedirs(sim_out_ts_dir, exist_ok=True)
        for i in range(0, self.num_upgrades + 1):
            os.makedirs(os.path.join(sim_out_ts_dir, f"up{i:02d}"), exist_ok=True)

        # Install custom gems to a volume that will be used by all workers
        # FIXME: Get working without docker
        if self.cfg.get("baseline", dict()).get("custom_gems", False):
            # TODO: Fix this stuff to work without docker
            logger.info("Installing custom gems to docker volume: buildstockbatch_custom_gems")

            docker_client = docker.client.from_env()

            # Create a volume to store the custom gems
            docker_client.volumes.create(name="buildstockbatch_custom_gems", driver="local")
            simdata_vol = docker_client.volumes.create(name="buildstockbatch_simdata_temp", driver="local")

            # Define directories to be mounted in the container
            mnt_gem_dir = "/var/oscli/gems"
            # Install custom gems to be used in the docker container
            local_gemfile_path = os.path.join(self.buildstock_dir, "resources", "Gemfile")
            mnt_gemfile_path_orig = "/var/oscli/gemfile/Gemfile"
            docker_volume_mounts = {
                "buildstockbatch_custom_gems": {"bind": mnt_gem_dir, "mode": "rw"},
                local_gemfile_path: {"bind": mnt_gemfile_path_orig, "mode": "ro"},
                simdata_vol.name: {"bind": "/var/simdata/openstudio", "mode": "rw"},
            }

            # Check that the Gemfile exists
            if not os.path.exists(local_gemfile_path):
                print(f"local_gemfile_path = {local_gemfile_path}")
                raise AttributeError("baseline:custom_gems = True, but did not find Gemfile in /resources directory")

            # Make the buildstock/resources/.custom_gems dir to store logs
            local_log_dir = os.path.join(self.buildstock_dir, "resources", ".custom_gems")
            if not os.path.exists(local_log_dir):
                os.makedirs(local_log_dir)

            # Run bundler to install the custom gems
            mnt_gemfile_path = f"{mnt_gem_dir}/Gemfile"
            bundle_install_cmd = f'/bin/bash -c "cp {mnt_gemfile_path_orig} {mnt_gemfile_path} && bundle install --path={mnt_gem_dir} --gemfile={mnt_gemfile_path}"'  # noqa: E501
            logger.debug(f"Running {bundle_install_cmd}")
            container_output = docker_client.containers.run(
                self.docker_image,
                bundle_install_cmd,
                remove=True,
                volumes=docker_volume_mounts,
                name="install_custom_gems",
            )
            with open(os.path.join(local_log_dir, "bundle_install_output.log"), "wb") as f_out:
                f_out.write(container_output)

            # Report out custom gems loaded by OpenStudio CLI
            check_active_gems_cmd = (
                f"openstudio --bundle {mnt_gemfile_path} --bundle_path {mnt_gem_dir} "
                "--bundle_without native_ext gem_list"
            )
            container_output = docker_client.containers.run(
                self.docker_image,
                check_active_gems_cmd,
                remove=True,
                volumes=docker_volume_mounts,
                name="list_custom_gems",
            )
            gem_list_log = os.path.join(local_log_dir, "openstudio_gem_list_output.log")
            with open(gem_list_log, "wb") as f_out:
                f_out.write(container_output)
            simdata_vol.remove()
            logger.debug(f"Review custom gems list at: {gem_list_log}")

    @classmethod
    def validate_project(cls, project_file):
        super(cls, cls).validate_project(project_file)
        # LocalBatch specific code goes here
        assert cls.validate_openstudio_path(project_file)

    @property
    def weather_dir(self):
        if self._weather_dir is None:
            self._weather_dir = os.path.join(self.buildstock_dir, "weather")
            self._get_weather_files()
        return self._weather_dir

    @classmethod
    def run_building(
        cls,
        buildstock_dir,
        weather_dir,
        results_dir,
        measures_only,
        n_datapoints,
        cfg,
        i,
        upgrade_idx=None,
    ):
        upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(results_dir, "simulation_output"))
        except SimulationExists:
            return
        sim_path = pathlib.Path(sim_dir)
        buildstock_path = pathlib.Path(buildstock_dir)

        # Make symlinks to project and buildstock stuff
        (sim_path / "measures").symlink_to(buildstock_path / "measures", target_is_directory=True)
        (sim_path / "lib").symlink_to(buildstock_path / "lib", target_is_directory=True)
        (sim_path / "weather").symlink_to(weather_dir, target_is_directory=True)
        hpxml_measures_path = buildstock_path / "resources" / "hpxml-measures"
        if hpxml_measures_path.exists():
            resources_path = sim_path / "resources"
            resources_path.mkdir()
            (resources_path / "hpxml-measures").symlink_to(hpxml_measures_path, target_is_directory=True)
        else:
            resources_path = None

        osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)

        with open(sim_path / "in.osw", "w") as f:
            json.dump(osw, f, indent=4)

        run_cmd = [
            cls.openstudio_exe(),
            "run",
            "-w",
            "in.osw",
        ]

        # FIXME: Custom gems
        # if cfg.get('baseline', dict()).get('custom_gems', False):
        #     run_cmd = [
        #         'openstudio',
        #         '--bundle', f'{mnt_custom_gem_dir}/Gemfile',
        #         '--bundle_path', f'{mnt_custom_gem_dir}',
        #         '--bundle_without', 'native_ext',
        #         'run', '-w', 'in.osw',
        #         '--debug'
        #     ]

        if measures_only:
            # if cfg.get('baseline', dict()).get('custom_gems', False):
            #     run_cmd.insert(8, '--measures_only')
            # else:
            run_cmd.insert(2, "--measures_only")

        env_vars = {}
        env_vars.update(os.environ)
        env_vars["BUILDSTOCKBATCH_VERSION"] = bsb_version

        max_time_min = cfg.get("max_minutes_per_sim")
        if max_time_min is not None:
            subprocess_kw = {"timeout": max_time_min * 60}
        else:
            subprocess_kw = {}
        start_time = dt.datetime.now()
        with open(sim_path / "openstudio_output.log", "w") as f_out:
            try:
                subprocess.run(
                    run_cmd,
                    check=True,
                    stdout=f_out,
                    stderr=subprocess.STDOUT,
                    env=env_vars,
                    cwd=sim_dir,
                    **subprocess_kw,
                )
            except subprocess.TimeoutExpired:
                end_time = dt.datetime.now()
                msg = f"Terminated {sim_id} after reaching max time of {max_time_min} minutes"
                logger.warning(msg)
                f_out.write(msg)
                with open(sim_path / "out.osw", "w") as out_osw:
                    out_msg = {
                        "started_at": start_time.strftime("%Y%m%dT%H%M%SZ"),
                        "completed_at": end_time.strftime("%Y%m%dT%H%M%SZ"),
                        "completed_status": "Fail",
                        "timeout": msg,
                    }
                    out_osw.write(json.dumps(out_msg, indent=3))
                (sim_path / "run").mkdir(exist_ok=True)
                with open(sim_path / "run" / "run.log", "a") as run_log:
                    run_log.write(f"[{end_time.strftime('%H:%M:%S')} ERROR] {msg}")
                with open(sim_path / "run" / "failed.job", "w") as failed_job:
                    failed_job.write(f"[{end_time.strftime('%H:%M:%S')} ERROR] {msg}")
                time.sleep(20)  # Wait for EnergyPlus to release file locks
            except subprocess.CalledProcessError:
                pass
            finally:
                fs = LocalFileSystem()
                cls.cleanup_sim_dir(
                    sim_dir,
                    fs,
                    f"{results_dir}/simulation_output/timeseries",
                    upgrade_id,
                    i,
                )

                # Clean up symlinks
                for directory in ("measures", "lib", "weather"):
                    (sim_path / directory).unlink()
                if resources_path:
                    (resources_path / "hpxml-measures").unlink()
                    resources_path.rmdir()

                # Read data_point_out.json
                reporting_measures = cls.get_reporting_measures(cfg)
                dpout = postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, i)
                return dpout

    def run_batch(self, n_jobs=None, measures_only=False, sampling_only=False):
        buildstock_csv_filename = self.sampler.run_sampling()

        if sampling_only:
            return

        # Copy files to lib dir in buildstock root
        # FIXME: does this work for comstock?
        buildstock_path = pathlib.Path(self.buildstock_dir)
        project_path = pathlib.Path(self.project_dir)
        lib_path = pathlib.Path(self.buildstock_dir, "lib")
        if lib_path.exists():
            shutil.rmtree(lib_path)
        lib_path.mkdir()
        shutil.copytree(buildstock_path / "resources", lib_path / "resources")
        shutil.copytree(
            project_path / "housing_characteristics",
            lib_path / "housing_characteristics",
        )

        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)

        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.buildstock_dir,
            self.weather_dir,
            self.results_dir,
            measures_only,
            n_datapoints,
            self.cfg,
        )
        upgrade_sims = []
        for i in range(self.num_upgrades):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), building_ids))
        if not self.skip_baseline_sims:
            baseline_sims = map(run_building_d, building_ids)
            all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        else:
            all_sims = itertools.chain(*upgrade_sims)
        if n_jobs is None:
            n_jobs = -1
        dpouts = Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

        time.sleep(10)
        shutil.rmtree(lib_path)

        sim_out_path = pathlib.Path(self.results_dir, "simulation_output")

        results_job_json_filename = sim_out_path / "results_job0.json.gz"
        with gzip.open(results_job_json_filename, "wt", encoding="utf-8") as f:
            json.dump(dpouts, f)
        del dpouts

        sim_out_tarfile_name = sim_out_path / "simulations_job0.tar.gz"
        logger.debug(f"Compressing simulation outputs to {sim_out_tarfile_name}")
        with tarfile.open(sim_out_tarfile_name, "w:gz") as tarf:
            for dirname in os.listdir(sim_out_path):
                if re.match(r"up\d+", dirname) and (sim_out_path / dirname).is_dir():
                    tarf.add(sim_out_path / dirname, arcname=dirname)
                    shutil.rmtree(sim_out_path / dirname)

    @property
    def output_dir(self):
        return self.results_dir

    @property
    def results_dir(self):
        results_dir = self.cfg.get("output_directory", os.path.join(self.project_dir, "localResults"))
        results_dir = self.path_rel_to_projectfile(results_dir)
        if not os.path.isdir(results_dir):
            os.makedirs(results_dir)
        return results_dir

    def get_dask_client(self):
        cluster = LocalCluster(local_directory=os.path.join(self.results_dir, "dask-tmp"))
        return Client(cluster)


@log_error_details()
def main():
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "defaultfmt": {
                    "format": "%(levelname)s:%(asctime)s:%(name)s:%(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "defaultfmt",
                    "level": "DEBUG",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                "__main__": {
                    "level": "DEBUG",
                    "propagate": True,
                    "handlers": ["console"],
                },
                "buildstockbatch": {
                    "level": "DEBUG",
                    "propagate": True,
                    "handlers": ["console"],
                },
            },
        }
    )
    parser = argparse.ArgumentParser()
    print(BuildStockBatchBase.LOGO)
    parser.add_argument("project_filename")
    parser.add_argument(
        "-j",
        type=int,
        help="Number of parallel simulations. Default: all cores.",
        default=None,
    )
    parser.add_argument(
        "-m",
        "--measures_only",
        action="store_true",
        help="Only apply the measures, but don't run simulations. Useful for debugging.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--postprocessonly",
        help="Only do postprocessing, useful for when the simulations are already done",
        action="store_true",
    )
    group.add_argument(
        "--uploadonly",
        help="Only upload to S3, useful when postprocessing is already done. Ignores the upload flag in yaml."
        " Errors out if files already exists in s3",
        action="store_true",
    )
    group.add_argument(
        "--continue_upload",
        help="Continue uploading to S3, useful when previous upload was interrupted.",
        action="store_true",
    )
    group.add_argument(
        "--validateonly",
        help="Only validate the project YAML file and references. Nothing is executed",
        action="store_true",
    )
    group.add_argument("--samplingonly", help="Run the sampling only.", action="store_true")
    args = parser.parse_args()
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError(f"The project file {args.project_filename} doesn't exist")

    # Validate the project, and in case of the --validateonly flag return True if validation passes
    LocalBatch.validate_project(args.project_filename)
    if args.validateonly:
        return
    batch = LocalBatch(args.project_filename)
    if not (args.postprocessonly or args.uploadonly or args.validateonly or args.continue_upload):
        batch.run_batch(
            n_jobs=args.j,
            measures_only=args.measures_only,
            sampling_only=args.samplingonly,
        )
    if args.measures_only or args.samplingonly:
        return
    if args.uploadonly:
        batch.process_results(skip_combine=True)
    elif args.continue_upload:
        batch.process_results(skip_combine=True, continue_upload=True)
    else:
        batch.process_results()


if __name__ == "__main__":
    main()
