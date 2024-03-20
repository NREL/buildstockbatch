# -*- coding: utf-8 -*-

"""
buildstockbatch.hpc
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with Eagle and Kestrel

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client
import datetime as dt
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import delayed, Parallel
import json
import logging
import math
import os
import pandas as pd
import pathlib
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import csv

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch.utils import (
    log_error_details,
    get_error_details,
    ContainerRuntime,
    path_rel_to_file,
    get_project_configuration,
    read_csv,
    get_bool_env_var,
)
from buildstockbatch import postprocessing
from buildstockbatch.__version__ import __version__ as bsb_version
from buildstockbatch.exc import ValidationError

logger = logging.getLogger(__name__)


class SlurmBatch(BuildStockBatchBase):
    DEFAULT_SYS_IMAGE_DIR = None
    HPC_NAME = None
    CORES_PER_NODE = None
    MIN_SIMS_PER_JOB = None
    CONTAINER_RUNTIME = ContainerRuntime.APPTAINER

    local_scratch = pathlib.Path(os.environ.get("LOCAL_SCRATCH", "/tmp/scratch"))
    local_buildstock_dir = local_scratch / "buildstock"
    local_weather_dir = local_scratch / "weather"
    local_output_dir = local_scratch / "output"
    local_apptainer_img = local_scratch / "openstudio.simg"
    local_housing_characteristics_dir = local_scratch / "housing_characteristics"

    def __init__(self, project_filename):
        super().__init__(project_filename)
        output_dir = pathlib.Path(self.output_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logger.debug("Output directory = {}".format(output_dir))
        weather_dir = self.weather_dir  # noqa E841

        self.apptainer_image = self.get_apptainer_image(self.cfg, self.os_version, self.os_sha)

    @classmethod
    def validate_project(cls, project_file):
        super().validate_project(project_file)
        cls.validate_apptainer_image_hpc(project_file)
        logger.info("HPC Validation Successful")
        return True

    @classmethod
    def validate_apptainer_image_hpc(cls, project_file):
        cfg = get_project_configuration(project_file)
        try:
            cls.get_apptainer_image(
                cfg,
                cfg.get("os_version", cls.DEFAULT_OS_VERSION),
                cfg.get("os_sha", cls.DEFAULT_OS_SHA),
            )
        except RuntimeError as err:
            raise ValidationError(str(err))

    @property
    def output_dir(self):
        output_dir = path_rel_to_file(self.project_filename, self.cfg["output_directory"])
        return output_dir

    @property
    def results_dir(self):
        results_dir = os.path.join(self.output_dir, "results")
        assert os.path.isdir(results_dir)
        return results_dir

    @staticmethod
    def clear_and_copy_dir(src, dst):
        if os.path.exists(dst):
            shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)

    @classmethod
    def get_apptainer_image(cls, cfg, os_version, os_sha):
        exts_to_try = ["Apptainer.sif", "Singularity.simg"]
        sys_img_dir = cfg.get("sys_image_dir", cls.DEFAULT_SYS_IMAGE_DIR)
        image_paths = [pathlib.Path(sys_img_dir, f"OpenStudio-{os_version}.{os_sha}-{ext}") for ext in exts_to_try]
        for image_path in image_paths:
            if image_path.exists():
                return str(image_path)
        raise RuntimeError(f"Could not find apptainer image: {' or '.join(map(str, image_paths))}")

    @property
    def weather_dir(self):
        weather_dir = os.path.join(self.output_dir, "weather")
        if not os.path.exists(weather_dir):
            os.makedirs(weather_dir)
            self._get_weather_files()
        return weather_dir

    def run_batch(self, sampling_only=False):
        # Create simulation_output dir
        sim_out_ts_dir = pathlib.Path(self.output_dir) / "results" / "simulation_output" / "timeseries"
        os.makedirs(sim_out_ts_dir, exist_ok=True)
        for i in range(0, len(self.cfg.get("upgrades", [])) + 1):
            os.makedirs(sim_out_ts_dir / f"up{i:02d}")

        # create destination_dir and copy housing_characteristics into it
        logger.debug("Copying housing characteristics")
        destination_dir = os.path.dirname(self.sampler.csv_path)
        if os.path.exists(destination_dir):
            shutil.rmtree(destination_dir)
        shutil.copytree(os.path.join(self.project_dir, "housing_characteristics"), destination_dir)
        logger.debug("Housing characteristics copied.")

        # run sampling
        buildstock_csv_filename = self.sampler.run_sampling()

        # Hit the weather_dir API to make sure that creating the weather directory isn't a race condition in the array
        # jobs - this is rare but happens quasi-repeatably when lustre is really lagging
        _ = self.weather_dir

        if sampling_only:
            return

        # Determine the number of simulations expected to be executed
        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)

        # find out how many buildings there are to simulate
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        # number of simulations is number of buildings * number of upgrades
        n_sims = n_datapoints * (len(self.cfg.get("upgrades", [])) + 1)

        # this is the number of simulations defined for this run as a "full job"
        #     number of simulations per job if we believe the .yml file n_jobs
        n_sims_per_job = math.ceil(n_sims / self.cfg[self.HPC_NAME]["n_jobs"])
        #     use more appropriate batch size in the case of n_jobs being much
        #     larger than we need, now that we know n_sims
        n_sims_per_job = max(n_sims_per_job, self.MIN_SIMS_PER_JOB)

        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get("upgrades", []))))
        if not self.skip_baseline_sims:
            # create batches of simulations
            baseline_sims = zip(building_ids, itertools.repeat(None))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        else:
            all_sims = list(itertools.chain(upgrade_sims))
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        for i in itertools.count(1):
            batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
            if not batch:
                break
            logger.info("Queueing job {} ({} simulations)".format(i, len(batch)))
            job_json_filename = os.path.join(self.output_dir, "job{:03d}.json".format(i))
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

        # now queue them
        jobids = self.queue_jobs()

        # queue up post-processing to run after all the simulation jobs are complete
        if not get_bool_env_var("MEASURESONLY"):
            self.queue_post_processing(jobids)

    def run_job_batch(self, job_array_number):
        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / "resources",
            self.local_buildstock_dir / "resources",
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / "measures",
            self.local_buildstock_dir / "measures",
        )
        if os.path.exists(pathlib.Path(self.buildstock_dir) / "resources/hpxml-measures"):
            self.clear_and_copy_dir(
                pathlib.Path(self.buildstock_dir) / "resources/hpxml-measures",
                self.local_buildstock_dir / "resources/hpxml-measures",
            )
        self.clear_and_copy_dir(self.weather_dir, self.local_weather_dir)
        self.clear_and_copy_dir(
            pathlib.Path(self.output_dir) / "housing_characteristics",
            self.local_housing_characteristics_dir,
        )
        if os.path.exists(self.local_apptainer_img):
            os.remove(self.local_apptainer_img)
        shutil.copy2(self.apptainer_image, self.local_apptainer_img)

        # Run the job batch as normal
        job_json_filename = os.path.join(self.output_dir, "job{:03d}.json".format(job_array_number))
        with open(job_json_filename, "r") as f:
            args = json.load(f)

        # trim the buildstock.csv file to only include rows for current batch. Helps speed up simulation
        logger.debug("Trimming buildstock.csv")
        building_ids = {x[0] for x in args["batch"]}
        buildstock_csv_path = self.local_housing_characteristics_dir / "buildstock.csv"
        valid_rows = []
        with open(buildstock_csv_path, "r", encoding="utf-8") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if int(row["Building"]) in building_ids:
                    valid_rows.append(row)
        df = pd.DataFrame.from_records(valid_rows)
        df.to_csv(buildstock_csv_path, index=False)
        logger.debug(f"Buildstock.csv trimmed to {len(df)} rows.")

        traceback_file_path = self.local_output_dir / "simulation_output" / f"traceback{job_array_number}.out"

        @delayed
        def run_building_d(i, upgrade_idx):
            try:
                return self.run_building(self.output_dir, self.cfg, args["n_datapoints"], i, upgrade_idx)
            except Exception:
                with open(traceback_file_path, "a") as f:
                    txt = get_error_details()
                    txt = "\n" + "#" * 20 + "\n" + f"Traceback for building{i}\n" + txt
                    f.write(txt)
                    del txt
                upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1
                return {"building_id": i, "upgrade": upgrade_id}

        # Run the simulations, get the data_point_out.json info from each
        tick = time.time()
        with Parallel(n_jobs=-1, verbose=9) as parallel:
            dpouts = parallel(itertools.starmap(run_building_d, args["batch"]))
        tick = time.time() - tick
        logger.info("Simulation time: {:.2f} minutes".format(tick / 60.0))

        # Save the aggregated dpouts as a json file
        lustre_sim_out_dir = pathlib.Path(self.results_dir) / "simulation_output"
        results_json = lustre_sim_out_dir / f"results_job{job_array_number}.json.gz"
        logger.info(f"Writing results to {results_json}")
        with gzip.open(results_json, "wt", encoding="utf-8") as f:
            json.dump(dpouts, f)

        # Compress simulation results
        if self.cfg.get("max_minutes_per_sim") is not None:
            time.sleep(60)  # Allow results JSON to finish writing
        simout_filename = lustre_sim_out_dir / f"simulations_job{job_array_number}.tar.gz"
        logger.info(f"Compressing simulation outputs to {simout_filename}")
        local_sim_out_dir = self.local_output_dir / "simulation_output"
        subprocess.run(
            [
                "tar",
                "cf",
                str(simout_filename),
                "-I",
                "pigz",
                "-C",
                str(local_sim_out_dir),
                ".",
            ],
            check=True,
        )

        # copy the tracebacks if it exists
        if os.path.exists(traceback_file_path):
            shutil.copy2(traceback_file_path, lustre_sim_out_dir)

        logger.info("batch complete")

        # Remove local scratch files
        dirs_to_remove = [
            self.local_buildstock_dir,
            self.local_weather_dir,
            self.local_output_dir,
            self.local_housing_characteristics_dir,
        ]

        logger.info(f"Cleaning up {self.local_scratch}")
        for dir in dirs_to_remove:
            logger.debug(f"Removing {dir}")
            if dir.exists():
                shutil.rmtree(dir)
            else:
                logger.warning(f"Directory does not exist {dir}")
        logger.debug(f"Removing {self.local_apptainer_img}")
        self.local_apptainer_img.unlink(missing_ok=True)

    @classmethod
    def run_building(cls, output_dir, cfg, n_datapoints, i, upgrade_idx=None):
        fs = LocalFileSystem()
        upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(cls.local_output_dir, "simulation_output"))
        except SimulationExists as ex:
            sim_dir = ex.sim_dir
        else:
            # Generate the osw for this simulation
            osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)
            with open(os.path.join(sim_dir, "in.osw"), "w") as f:
                json.dump(osw, f, indent=4)

            # Create a temporary directory for the simulation to use
            with tempfile.TemporaryDirectory(dir=cls.local_scratch, prefix=f"{sim_id}_") as tmpdir:
                # Build the command to instantiate and configure the apptainer container the simulation is run inside
                local_resources_dir = cls.local_buildstock_dir / "resources"
                args = [
                    "apptainer",
                    "exec",
                    "--contain",
                    "-e",
                    "--pwd",
                    "/var/simdata/openstudio",
                    "-B",
                    f"{sim_dir}:/var/simdata/openstudio",
                    "-B",
                    f"{local_resources_dir}:/lib/resources",
                    "-B",
                    f"{cls.local_housing_characteristics_dir}:/lib/housing_characteristics",
                    "-B",
                    f"{tmpdir}:/tmp",
                ]
                runscript = ["ln -s /lib /var/simdata/openstudio/lib"]

                # Copy other necessary stuff into the simulation directory
                dirs_to_mount = [
                    cls.local_buildstock_dir / "measures",
                    cls.local_weather_dir,
                ]

                for src in dirs_to_mount:
                    container_mount = "/" + src.name
                    args.extend(["-B", "{}:{}:ro".format(src, container_mount)])
                    container_symlink = pathlib.Path("/var/simdata/openstudio", src.name)
                    runscript.append("ln -s {} {}".format(*map(shlex.quote, (container_mount, str(container_symlink)))))

                if (cls.local_buildstock_dir / "resources" / "hpxml-measures").exists():
                    runscript.append("ln -s /resources /var/simdata/openstudio/resources")
                    src = cls.local_buildstock_dir / "resources" / "hpxml-measures"
                    container_mount = "/resources/hpxml-measures"
                    args.extend(["-B", f"{src}:{container_mount}:ro"])

                # Build the openstudio command that will be issued within the
                # apptainer container If custom gems are to be used in the
                # apptainer container add extra bundle arguments to the cli
                # command
                cli_cmd = "openstudio run -w in.osw"
                if cfg.get("baseline", dict()).get("custom_gems", False):
                    cli_cmd = (
                        "openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems "
                        "--bundle_without native_ext run -w in.osw --debug"
                    )
                if get_bool_env_var("MEASURESONLY"):
                    cli_cmd += " --measures_only"
                runscript.append(cli_cmd)
                args.extend([str(cls.local_apptainer_img), "bash", "-x"])
                env_vars = dict(os.environ)
                env_vars["SINGULARITYENV_BUILDSTOCKBATCH_VERSION"] = bsb_version
                logger.debug("\n".join(map(str, args)))
                max_time_min = cfg.get("max_minutes_per_sim")
                if max_time_min is not None:
                    subprocess_kw = {"timeout": max_time_min * 60}
                else:
                    subprocess_kw = {}
                start_time = dt.datetime.now()
                with open(pathlib.Path(sim_dir, "openstudio_output.log"), "w") as f_out:
                    try:
                        subprocess.run(
                            args,
                            check=True,
                            input="\n".join(runscript).encode("utf-8"),
                            stdout=f_out,
                            stderr=subprocess.STDOUT,
                            cwd=cls.local_output_dir,
                            env=env_vars,
                            **subprocess_kw,
                        )
                    except subprocess.TimeoutExpired:
                        end_time = dt.datetime.now()
                        msg = f"Terminated {sim_id} after reaching max time of {max_time_min} minutes"
                        f_out.write(f"[{end_time.now()} ERROR] {msg}")
                        logger.warning(msg)
                        with open(pathlib.Path(sim_dir, "out.osw"), "w") as out_osw:
                            out_msg = {
                                "started_at": start_time.strftime("%Y%m%dT%H%M%SZ"),
                                "completed_at": end_time.strftime("%Y%m%dT%H%M%SZ"),
                                "completed_status": "Fail",
                                "timeout": msg,
                            }
                            out_osw.write(json.dumps(out_msg, indent=3))
                        with open(pathlib.Path(sim_dir, "run", "out.osw"), "a") as run_log:
                            run_log.write(f"[{end_time.strftime('%H:%M:%S')} ERROR] {msg}")
                        with open(pathlib.Path(sim_dir, "run", "failed.job"), "w") as failed_job:
                            failed_job.write(f"[{end_time.strftime('%H:%M:%S')} ERROR] {msg}")
                        # Wait for EnergyPlus to release file locks and data_point.zip to finish
                        time.sleep(60)
                    except subprocess.CalledProcessError:
                        pass
                    finally:
                        # Clean up the symbolic links we created in the container
                        for mount_dir in dirs_to_mount + [pathlib.Path(sim_dir, "lib")]:
                            try:
                                pathlib.Path(sim_dir, os.path.basename(mount_dir)).unlink()
                            except FileNotFoundError:
                                pass

                        # Clean up simulation directory
                        cls.cleanup_sim_dir(
                            sim_dir,
                            fs,
                            f"{output_dir}/results/simulation_output/timeseries",
                            upgrade_id,
                            i,
                        )

        reporting_measures = cls.get_reporting_measures(cfg)
        dpout = postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, i)
        return dpout

    @staticmethod
    def _queue_jobs_env_vars() -> dict:
        raise NotImplementedError()

    @classmethod
    def queue_sampling(
        cls,
        project_filename: os.PathLike,
        measures_only: bool,
        sampling_only: bool,
        hipri: bool,
    ):
        cfg = get_project_configuration(project_filename)
        hpc_sh = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"{cls.HPC_NAME}.sh")
        assert os.path.exists(hpc_sh)
        out_dir = cfg["output_directory"]
        if os.path.exists(out_dir):
            raise FileExistsError(
                "The output directory {} already exists. Please delete it or choose another.".format(out_dir)
            )
        logger.info("Creating output directory {}".format(out_dir))
        os.makedirs(out_dir)
        env_export = {}
        env_export["PROJECTFILE"] = project_filename
        env_export["MEASURESONLY"] = str(int(measures_only))
        env_export["SAMPLINGONLY"] = str(int(sampling_only))
        env_export.update(cls._queue_jobs_env_vars())
        env = {}
        env.update(os.environ)
        env.update(env_export)
        subargs = [
            "sbatch",
            "--time={}".format(cfg[cls.HPC_NAME].get("sampling", {}).get("time", 60)),
            "--account={}".format(cfg[cls.HPC_NAME]["account"]),
            "--nodes=1",
            "--export={}".format(",".join(env.keys())),
            "--output=sampling.out",
            hpc_sh,
        ]
        if hipri:
            subargs.insert(-1, "--qos=high")
        logger.info("Submitting sampling job to task scheduler")
        subprocess.run(subargs, env=env, cwd=out_dir, check=True)
        logger.info("Run squeue -u $USER to monitor the progress of your jobs")

    def queue_jobs(self, array_ids=None, hipri=False):
        hpc_cfg = self.cfg[self.HPC_NAME]
        with open(pathlib.Path(self.output_dir, "job001.json"), "r") as f:
            job_json = json.load(f)
            n_sims_per_job = len(job_json["batch"])
            del job_json
        if array_ids:
            array_spec = ",".join(map(str, array_ids))
        else:
            jobjson_re = re.compile(r"job(\d+).json")
            array_max = max(
                map(
                    lambda m: int(m.group(1)),
                    filter(
                        lambda m: m is not None,
                        map(jobjson_re.match, (os.listdir(self.output_dir))),
                    ),
                )
            )
            array_spec = "1-{}".format(array_max)
        account = hpc_cfg["account"]

        # Estimate the wall time in minutes
        minutes_per_sim = hpc_cfg["minutes_per_sim"]
        walltime = math.ceil(math.ceil(n_sims_per_job / self.CORES_PER_NODE) * minutes_per_sim)

        # Queue up simulations
        here = os.path.dirname(os.path.abspath(__file__))
        hpc_sh = os.path.join(here, f"{self.HPC_NAME}.sh")
        env = {}
        env.update(os.environ)
        env["PROJECTFILE"] = self.project_filename
        extra_env_vars = self._queue_jobs_env_vars()
        env.update(extra_env_vars)
        export_vars = ["PROJECTFILE", "MEASURESONLY"]
        export_vars.extend(extra_env_vars.keys())
        args = [
            "sbatch",
            "--account={}".format(account),
            "--time={}".format(walltime),
            "--export={}".format(",".join(export_vars)),
            "--array={}".format(array_spec),
            "--output=job.out-%a",
            "--job-name=bstk",
            hpc_sh,
        ]
        if os.environ.get("SLURM_JOB_QOS"):
            args.insert(-1, "--qos={}".format(os.environ.get("SLURM_JOB_QOS")))
        elif hipri:
            args.insert(-1, "--qos=high")

        logger.debug(" ".join(args))
        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            encoding="utf-8",
            cwd=self.output_dir,
        )
        try:
            resp.check_returncode()
        except subprocess.CalledProcessError as ex:
            logger.error(ex.stderr)
            raise
        for line in resp.stdout.split("\n"):
            logger.debug("sbatch:" + line)
        m = re.search(r"Submitted batch job (\d+)", resp.stdout)
        if not m:
            logger.error("Did not receive job id back from sbatch:")
            raise RuntimeError("Didn't receive job id back from sbatch")
        job_id = m.group(1)
        return [job_id]

    def queue_post_processing(self, after_jobids=[], upload_only=False, hipri=False):
        # Configuration values
        hpc_cfg = self.cfg[self.HPC_NAME]
        account = hpc_cfg["account"]
        walltime = hpc_cfg.get("postprocessing", {}).get("time", "1:30:00")
        memory = hpc_cfg.get("postprocessing", {}).get("node_memory_mb", self.DEFAULT_POSTPROCESSING_NODE_MEMORY_MB)
        n_procs = hpc_cfg.get("postprocessing", {}).get("n_procs", self.DEFAULT_POSTPROCESSING_N_PROCS)
        n_workers = hpc_cfg.get("postprocessing", {}).get("n_workers", self.DEFAULT_POSTPROCESSING_N_WORKERS)
        print(f"Submitting job to {n_workers} {memory}MB memory nodes using {n_procs} cores in each.")
        # Throw an error if the files already exist.

        if not upload_only:
            for subdir in ("parquet", "results_csvs"):
                subdirpath = pathlib.Path(self.output_dir, "results", subdir)
                if subdirpath.exists():
                    raise FileExistsError(
                        f"{subdirpath} already exists. This means you may have run postprocessing already. If you are sure you want to rerun, delete that directory and try again."
                    )  # noqa E501

        # Move old output logs and config to make way for new ones
        for filename in (
            "dask_scheduler.json",
            "dask_scheduler.out",
            "dask_workers.out",
            "postprocessing.out",
        ):
            filepath = pathlib.Path(self.output_dir, filename)
            if filepath.exists():
                last_mod_date = dt.datetime.fromtimestamp(os.path.getmtime(filepath))
                shutil.move(
                    filepath,
                    filepath.parent / f"{filepath.stem}_{last_mod_date:%Y%m%d%H%M}{filepath.suffix}",
                )

        env_export = {
            "PROJECTFILE": self.project_filename,
            "OUT_DIR": self.output_dir,
            "UPLOADONLY": str(upload_only),
            "MEMORY": str(memory),
            "NPROCS": str(n_procs),
        }
        env_export.update(self._queue_jobs_env_vars())
        here = os.path.dirname(os.path.abspath(__file__))
        hpc_post_sh = os.path.join(here, f"{self.HPC_NAME}_postprocessing.sh")

        args = [
            "sbatch",
            "--account={}".format(account),
            "--time={}".format(walltime),
            "--export={}".format(",".join(env_export.keys())),
            "--job-name=bstkpost",
            "--output=postprocessing.out",
            "--nodes=1",
            ":",
            "--mem={}".format(memory),
            "--output=dask_workers.out",
            "--nodes={}".format(n_workers),
            hpc_post_sh,
        ]

        if after_jobids:
            args.insert(4, "--dependency=afterany:{}".format(":".join(after_jobids)))

        if os.environ.get("SLURM_JOB_QOS"):
            args.insert(-1, "--qos={}".format(os.environ.get("SLURM_JOB_QOS")))
        elif hipri:
            args.insert(-1, "--qos=high")

        env = {}
        env.update(os.environ)
        env.update(env_export)
        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            encoding="utf-8",
            cwd=self.output_dir,
        )
        for line in resp.stdout.split("\n"):
            logger.debug("sbatch: {}".format(line))

    def get_dask_client(self):
        # Keep this, helpful for debugging on a bigmem node
        # from dask.distributed import LocalCluster
        # cluster = LocalCluster(local_directory="/tmp/scratch/dask", n_workers=90, memory_limit="16GiB")
        # return Client(cluster)
        return Client(scheduler_file=os.path.join(self.output_dir, "dask_scheduler.json"))

    def process_results(self, *args, **kwargs):
        # Check that all the jobs succeeded before proceeding
        failed_job_array_ids = self.get_failed_job_array_ids()
        if failed_job_array_ids:
            logger.error("The following simulation jobs failed: {}".format(", ".join(map(str, failed_job_array_ids))))
            logger.error("Please inspect those jobs and fix any problems before resubmitting.")
            logger.critical("Postprocessing cancelled.")
            return False

        super().process_results(*args, **kwargs)

    def _get_job_ids_for_file_pattern(self, pat):
        job_ids = set()
        for filename in os.listdir(self.output_dir):
            m = re.search(pat, filename)
            if not m:
                continue
            job_ids.add(int(m.group(1)))
        return job_ids

    def get_failed_job_array_ids(self):
        job_out_files = sorted(pathlib.Path(self.output_dir).glob("job.out-*"))

        failed_job_ids = set()
        for filename in job_out_files:
            with open(filename, "r") as f:
                if not re.search(r"batch complete", f.read()):
                    job_id = int(re.match(r"job\.out-(\d+)", filename.name).group(1))
                    logger.debug(f"Array Job ID {job_id} had a failure.")
                    failed_job_ids.add(job_id)

        job_out_ids = self._get_job_ids_for_file_pattern(r"job\.out-(\d+)")
        job_json_ids = self._get_job_ids_for_file_pattern(r"job(\d+)\.json")
        missing_job_ids = job_json_ids - job_out_ids
        failed_job_ids.update(missing_job_ids)

        return sorted(failed_job_ids)

    def rerun_failed_jobs(self, hipri=False):
        # Find the jobs that failed
        failed_job_array_ids = self.get_failed_job_array_ids()
        if not failed_job_array_ids:
            logger.error("There are no failed jobs to rerun. Exiting.")
            return False

        output_path = pathlib.Path(self.output_dir)
        results_path = pathlib.Path(self.results_dir)

        prev_failed_job_out_dir = output_path / "prev_failed_jobs"
        os.makedirs(prev_failed_job_out_dir, exist_ok=True)
        for job_array_id in failed_job_array_ids:
            # Move the failed job.out file so it doesn't get overwritten
            filepath = output_path / f"job.out-{job_array_id}"
            if filepath.exists():
                last_mod_date = dt.datetime.fromtimestamp(os.path.getmtime(filepath))
                shutil.move(
                    filepath,
                    prev_failed_job_out_dir / f"{filepath.name}_{last_mod_date:%Y%m%d%H%M}",
                )

            # Delete simulation results for jobs we're about to rerun
            files_to_delete = [
                f"simulations_job{job_array_id}.tar.gz",
                f"results_job{job_array_id}.json.gz",
            ]
            for filename in files_to_delete:
                (results_path / "simulation_output" / filename).unlink(missing_ok=True)

        # Clear out postprocessed data so we can start from a clean slate
        dirs_to_delete = [results_path / "results_csvs", results_path / "parquet"]
        for x in dirs_to_delete:
            if x.exists():
                shutil.rmtree(x)

        job_ids = self.queue_jobs(failed_job_array_ids, hipri=hipri)
        self.queue_post_processing(job_ids, hipri=hipri)


class EagleBatch(SlurmBatch):
    DEFAULT_SYS_IMAGE_DIR = "/shared-projects/buildstock/singularity_images"
    HPC_NAME = "eagle"
    CORES_PER_NODE = 36
    MIN_SIMS_PER_JOB = 36 * 2
    DEFAULT_POSTPROCESSING_NODE_MEMORY_MB = 85248
    DEFAULT_POSTPROCESSING_N_PROCS = 18
    DEFAULT_POSTPROCESSING_N_WORKERS = 2

    @classmethod
    def validate_output_directory_eagle(cls, project_file):
        cfg = get_project_configuration(project_file)
        output_dir = path_rel_to_file(project_file, cfg["output_directory"])
        if not re.match(r"/(lustre/eaglefs/)?(scratch|projects)", output_dir):
            raise ValidationError(
                f"`output_directory` must be in /scratch or /projects," f" `output_directory` = {output_dir}"
            )

    @classmethod
    def validate_project(cls, project_file):
        super(cls, cls).validate_project(project_file)
        cls.validate_output_directory_eagle(project_file)
        logger.info("Eagle Validation Successful")
        return True

    @staticmethod
    def _queue_jobs_env_vars() -> dict:
        env = {"MY_CONDA_ENV": os.environ["CONDA_PREFIX"]}
        return env


class KestrelBatch(SlurmBatch):
    DEFAULT_SYS_IMAGE_DIR = "/kfs2/shared-projects/buildstock/apptainer_images"
    HPC_NAME = "kestrel"
    CORES_PER_NODE = 104
    MIN_SIMS_PER_JOB = 104 * 2
    DEFAULT_POSTPROCESSING_NODE_MEMORY_MB = 250000  # Standard node
    DEFAULT_POSTPROCESSING_N_PROCS = 52
    DEFAULT_POSTPROCESSING_N_WORKERS = 2

    @classmethod
    def validate_output_directory_kestrel(cls, project_file):
        cfg = get_project_configuration(project_file)
        output_dir = path_rel_to_file(project_file, cfg["output_directory"])
        if not re.match(r"/(kfs\d/)?(scratch|projects)", output_dir):
            raise ValidationError(
                f"`output_directory` must be in /scratch or /projects," f" `output_directory` = {output_dir}"
            )

    @classmethod
    def validate_project(cls, project_file):
        super().validate_project(project_file)
        cls.validate_output_directory_kestrel(project_file)
        logger.info("Kestrel Validation Successful")
        return True

    @staticmethod
    def _queue_jobs_env_vars() -> dict:
        env = {"MY_PYTHON_ENV": os.environ["VIRTUAL_ENV"]}
        return env


logging_config = {
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
        "__main__": {"level": "DEBUG", "propagate": True, "handlers": ["console"]},
        "buildstockbatch": {
            "level": "DEBUG",
            "propagate": True,
            "handlers": ["console"],
        },
    },
}


def eagle_cli(argv=sys.argv[1:]):
    user_cli(EagleBatch, argv)


def kestrel_cli(argv=sys.argv[1:]):
    user_cli(KestrelBatch, argv)


def user_cli(Batch: SlurmBatch, argv: list):
    """
    This is the user entry point for running buildstockbatch on Eagle/Kestrel
    """
    # set up logging, currently based on within-this-file hard-coded config
    logging.config.dictConfig(logging_config)

    # print BuildStockBatch logo
    print(BuildStockBatchBase.LOGO)

    # CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("project_filename")
    parser.add_argument(
        "--hipri",
        action="store_true",
        help="Submit this job to the high priority queue. Uses 2x node hours.",
    )
    parser.add_argument(
        "-m",
        "--measuresonly",
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
        help="Only upload to S3, useful when postprocessing is already done. Ignores the upload flag in yaml",
        action="store_true",
    )
    group.add_argument(
        "--validateonly",
        help="Only validate the project YAML file and references. Nothing is executed",
        action="store_true",
    )
    group.add_argument("--samplingonly", help="Run the sampling only.", action="store_true")
    group.add_argument("--rerun_failed", help="Rerun the failed jobs", action="store_true")

    # parse CLI arguments
    args = parser.parse_args(argv)

    # load the yaml project file
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError("The project file {} doesn't exist".format(args.project_filename))
    project_filename = os.path.abspath(args.project_filename)

    # validate the project, and in case of the --validateonly flag return True if validation passes
    Batch.validate_project(project_filename)
    if args.validateonly:
        return True

    # if the project has already been run, simply queue the correct post-processing step
    if args.postprocessonly or args.uploadonly:
        batch = Batch(project_filename)
        batch.queue_post_processing(upload_only=args.uploadonly, hipri=args.hipri)
        return True

    if args.rerun_failed:
        batch = Batch(project_filename)
        batch.rerun_failed_jobs(hipri=args.hipri)
        return True

    # otherwise, queue up the whole buildstockbatch process
    # the main work of the first job is to run the sampling script ...
    Batch.queue_sampling(project_filename, args.measuresonly, args.samplingonly, args.hipri)


@log_error_details()
def main():
    """
    Determines which piece of work is to be run right now, on this process, on
    this node. There are four types of work that may need to be done:

    - initialization, sampling, and queuing other work (job_array_number == 0)
    - run a batch of simulations (job_array_number > 0)
    - post-process results (job_array_number == 0 and POSTPROCESS)
    - upload results to Athena (job_array_number == 0 and POSTPROCESS and UPLOADONLY)

    The context for the work is deinfed by the project_filename (project .yml file),
    which is used to initialize an EagleBatch object.
    """

    # set up logging, currently based on within-this-file hard-coded config
    logging.config.dictConfig(logging_config)

    # only direct script argument is the project .yml file
    parser = argparse.ArgumentParser()
    parser.add_argument("hpc_name", choices=["eagle", "kestrel"])
    parser.add_argument("project_filename")
    args = parser.parse_args()

    # initialize the EagleBatch/KestrelBatch object
    if args.hpc_name == "eagle":
        batch = EagleBatch(args.project_filename)
    else:
        assert args.hpc_name == "kestrel"
        batch = KestrelBatch(args.project_filename)
    # other arguments/cues about which part of the process we are in are
    # encoded in slurm job environment variables
    job_array_number = int(os.environ.get("SLURM_ARRAY_TASK_ID", 0))
    post_process = get_bool_env_var("POSTPROCESS")
    upload_only = get_bool_env_var("UPLOADONLY")
    measures_only = get_bool_env_var("MEASURESONLY")
    sampling_only = get_bool_env_var("SAMPLINGONLY")
    if job_array_number:
        # if job array number is non-zero, run the batch job
        # Simulation should not be scheduled for sampling only
        assert not sampling_only
        batch.run_job_batch(job_array_number)
    elif post_process:
        logger.debug("Starting postprocessing")
        # else, we might be in a post-processing step
        # Postprocessing should not have been scheduled if measures only or sampling only are run
        assert not measures_only
        assert not sampling_only
        if upload_only:
            batch.process_results(skip_combine=True)
        else:
            batch.process_results()
    else:
        logger.debug("Kicking off batch")
        # default job_array_number == 0 task is to kick the whole BuildStock
        # process off, that is, to create samples and then create batch jobs
        # to run them
        batch.run_batch(sampling_only)


if __name__ == "__main__":
    bsb_cli = os.environ.get("BUILDSTOCKBATCH_CLI")
    if bsb_cli == "eagle":
        eagle_cli()
    elif bsb_cli == "kestrel":
        kestrel_cli()
    else:
        main()
