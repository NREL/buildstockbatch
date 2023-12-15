# -*- coding: utf-8 -*-

"""
buildstockbatch.gcp
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with GCP Batch.

This implementation tries to match the structure of `../aws/aws.py` in the 'nrel/aws_batch' branch
as much as possible in order to make it easier to refactor these two (or three, with Eagle) to share
code later. Also, because that branch has not yet been merged, this will also _not_ do any
refactoring right now to share code with that (to reduce merging complexity later). Instead, code
that's likely to be refactored out will be commented with 'todo: aws-shared'.

:author: Robert LaThanh, Natalie Weires
:copyright: (c) 2023 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import argparse
import collections
from dask.distributed import Client as DaskClient
from datetime import datetime
from gcsfs import GCSFileSystem
import gzip
from joblib import Parallel, delayed
import json
import io
import logging
import os
import pathlib
import re
import shutil
import tarfile
import time
import tqdm

from google.api_core import exceptions
from google.cloud import artifactregistry_v1
from google.cloud import batch_v1, storage
from google.cloud.storage import transfer_manager
from google.cloud import compute_v1
from google.cloud import run_v2

from buildstockbatch import postprocessing
from buildstockbatch.cloud import docker_base
from buildstockbatch.cloud.docker_base import DockerBatchBase
from buildstockbatch.exc import ValidationError
from buildstockbatch.utils import (
    get_project_configuration,
    log_error_details,
)


logger = logging.getLogger(__name__)


def upload_directory_to_GCS(local_directory, bucket, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    local_dir_abs = pathlib.Path(local_directory).absolute()

    string_paths = []
    for dirpath, dirnames, filenames in os.walk(local_dir_abs):
        for filename in filenames:
            if filename.startswith("."):
                continue
            local_filepath = pathlib.Path(dirpath, filename)
            string_paths.append(str(local_filepath.relative_to(local_dir_abs)))

    transfer_manager.upload_many_from_filenames(
        bucket,
        string_paths,
        source_directory=local_dir_abs,
        blob_name_prefix=prefix,
        raise_exception=True,
    )


def copy_GCS_file(src_bucket, src_name, dest_bucket, dest_name):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(src_bucket)
    source_blob = source_bucket.blob(src_name)
    destination_bucket = storage_client.bucket(dest_bucket)
    source_bucket.copy_blob(source_blob, destination_bucket, dest_name)


def delete_job(job_name):
    """Delete an existing GCP Batch job, with user confirmation.

    :param job_name: Full GCP Batch job name (projects/{project}/locations/{region}/jobs/{name})
    """
    client = batch_v1.BatchServiceClient()
    try:
        job_info = client.get_job(batch_v1.GetJobRequest(name=job_name))
    except exceptions.NotFound:
        logger.error(f"Job {job_name} not found.")
        return
    except Exception:
        logger.error(
            f"Job {job_name} invalid or not found. Job name should be in the format "
            "projects/{project}/locations/{region}/jobs/{name}."
        )
        return

    job_status = job_info.status.state.name
    answer = input(
        f"Current status of job {job_name} is {job_status}. Are you sure you want to cancel and delete it? (y/n) "
    )
    if answer[:1] not in ("y", "Y"):
        return

    request = batch_v1.DeleteJobRequest(
        name=job_name,
    )
    operation = client.delete_job(request=request)
    logger.info("Canceling and deleting GCP Batch job. This may take a few minutes.")
    operation.result()


class TsvLogger:
    """Collects pairs of headers and values, and then outputs to the logger the set of headers on
    one line and the set of values on another.

    The entries (of the headers and values) are separated by tabs (for easy pasting into a
    spreadsheet), and may also have spaces for padding so headers and values line up in logging
    output.
    """

    def __init__(self):
        self.headers, self.values = [], []

    def append_stat(self, header, value):
        width = max(len(str(header)), len(str(value)))
        self.headers.append(str(header).rjust(width))
        self.values.append(str(value).rjust(width))

    def log_stats(self, level):
        logger.log(level, "\t".join(self.headers))
        logger.log(level, "\t".join(self.values))


class GcpBatch(DockerBatchBase):
    # https://patorjk.com/software/taag/#p=display&f=Santa%20Clara&t=BuildStockBatch%20%20%2F%20GCP
    LOGO = """
     _ __         _     __,              _ __                      /     ,___ ,___ _ __
    ( /  )    o  //   /(    _/_       / ( /  )     _/_    /       /     /   //   /( /  )
     /--< , ,,  // __/  `.  /  __ _, /<  /--< __,  /  _, /_      /     /  __/      /--'
    /___/(_/_(_(/_(_/_(___)(__(_)(__/ |_/___/(_/(_(__(__/ /_    /     (___/(___/  /
      Executing BuildStock projects with grace since 2018
"""
    # Default post-processing resources
    DEFAULT_PP_CPUS = 2
    DEFAULT_PP_MEMORY_MIB = 4096

    def __init__(self, project_filename, job_identifier=None):
        """
        :param project_filename: Path to the project's configuration file.
        :param job_identifier: Optional job ID that will override gcp.job_identifier from the project file.
        """
        super().__init__(project_filename)

        if job_identifier:
            assert len(job_identifier) <= 48, "Job identifier must be no longer than 48 characters."
            assert re.match(
                "^[a-z]([a-z0-9-]{0,46}[a-z0-9])?$", job_identifier
            ), "Job identifer must start with a letter and contain only letters, numbers, and hyphens."
            self.job_identifier = job_identifier
        else:
            self.job_identifier = self.cfg["gcp"]["job_identifier"]

        self.project_filename = project_filename
        self.gcp_project = self.cfg["gcp"]["project"]
        self.region = self.cfg["gcp"]["region"]
        self.ar_repo = self.cfg["gcp"]["artifact_registry"]["repository"]
        self.gcs_bucket = self.cfg["gcp"]["gcs"]["bucket"]
        self.gcs_prefix = self.cfg["gcp"]["gcs"]["prefix"]
        self.batch_array_size = self.cfg["gcp"]["batch_array_size"]

    @staticmethod
    def get_AR_repo_name(gcp_project, region, repo):
        """Returns the full name of a repository in Artifact Registry."""
        return f"projects/{gcp_project}/locations/{region}/repositories/{repo}"

    @staticmethod
    def validate_gcp_args(project_file):
        cfg = get_project_configuration(project_file)
        assert "gcp" in cfg, 'Project config must contain a "gcp" section'
        gcp_project = cfg["gcp"]["project"]

        # Check that GCP region exists and is available for this project
        region = cfg["gcp"]["region"]
        regions_client = compute_v1.RegionsClient()
        try:
            regions_client.get(project=gcp_project, region=region)
        except exceptions.NotFound:
            raise ValidationError(
                f"Region {region} is not available in project {gcp_project}. "
                '(Region should be something like "us-central1" or "asia-east2")'
            )

        # Check that GCP bucket exists
        bucket = cfg["gcp"]["gcs"]["bucket"]
        storage_client = storage.Client(project=gcp_project)
        assert storage_client.bucket(bucket).exists(), f"GCS bucket {bucket} does not exist in project {gcp_project}"

        # Check that artifact registry repository exists
        repo = cfg["gcp"]["artifact_registry"]["repository"]
        ar_client = artifactregistry_v1.ArtifactRegistryClient()
        repo_name = GcpBatch.get_AR_repo_name(gcp_project, region, repo)
        try:
            ar_client.get_repository(name=repo_name)
        except exceptions.NotFound:
            raise ValidationError(
                f"Artifact Registry repository {repo} does not exist in project {gcp_project} and region {region}"
            )

        # Check post-processing resources
        pp_env = cfg["gcp"].get("postprocessing_environment")
        if pp_env:
            cpus = pp_env.get("cpus", GcpBatch.DEFAULT_PP_CPUS)
            memory = pp_env.get("memory_mib", GcpBatch.DEFAULT_PP_MEMORY_MIB)

            # Allowable values are documented at:
            # https://cloud.google.com/python/docs/reference/run/latest/google.cloud.run_v2.types.ResourceRequirements
            cpus_to_memory_limits = {
                1: (512, 4096),
                2: (512, 8192),
                4: (2048, 16384),
                8: (4096, 32768),
            }

            assert cpus in cpus_to_memory_limits, "gcp.postprocessing_environment.cpus must be 1, 2, 4 or 8."
            min_memory, max_memory = cpus_to_memory_limits[cpus]
            assert memory >= min_memory, (
                f"gcp.postprocessing_environment.memory_mib must be at least {min_memory} for {cpus} CPUs. "
                f"(Found {memory}) See https://cloud.google.com/run/docs/configuring/services/cpu"
            )
            assert memory <= max_memory, (
                f"gcp.postprocessing_environment.memory_mib must be less than or equal to {max_memory} for {cpus} CPUs "
                f"(Found {memory}) See https://cloud.google.com/run/docs/configuring/services/memory-limits"
            )

    @staticmethod
    def validate_project(project_file):
        super(GcpBatch, GcpBatch).validate_project(project_file)
        GcpBatch.validate_gcp_args(project_file)

    @property
    def docker_image(self):
        return "nrel/buildstockbatch"

    # todo: aws-shared (see file comment)
    @property
    def weather_dir(self):
        return self._weather_dir

    @property
    def results_dir(self):
        return f"{self.gcs_bucket}/{self.gcs_prefix}/results"

    @property
    def registry_url(self):
        """
        The registry that the image(s) will be pushed to.

        :returns: A string of a GCP Artifact Repository URL; for example,
            `https://us-central1-docker.pkg.dev
        """
        return f"https://{self.repository_uri.split('/')[0]}"

    @property
    def repository_uri(self):
        """
        The "repository" (name) for this image for pushing to a GCP Artifact
        Repository.

        :returns: A string for this image given the Artifact Repository (given
            its region, project name, and repo name), followed by
            "buildstockbatch"; for example,
             `us-central1-docker.pkg.dev/buildstockbatch/buildstockbatch-docker/buildstockbatch`
        """
        return f"{self.region}-docker.pkg.dev/{self.gcp_project}/{self.ar_repo}/buildstockbatch"

    @property
    def postprocessing_job_id(self):
        return f"{self.job_identifier}-pp"

    @property
    def postprocessing_job_name(self):
        return f"projects/{self.gcp_project}/locations/{self.region}/jobs/{self.postprocessing_job_id}"

    @property
    def postprocessing_job_console_url(self):
        return (
            f"https://console.cloud.google.com/run/jobs/details/{self.region}"
            f"/{self.postprocessing_job_id}/executions?project={self.gcp_project}"
        )

    # todo: aws-shared (see file comment)
    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / "Dockerfile").exists():
            raise RuntimeError(f"The needs to be run from the root of the repo, found {root_path}")

        # Make the buildstock/resources/.gcp_docker_image dir to store logs
        local_log_dir = os.path.join(self.buildstock_dir, "resources", ".gcp_docker_image")
        if not os.path.exists(local_log_dir):
            os.makedirs(local_log_dir)

        # Determine whether or not to build the image with custom gems bundled in
        if self.cfg.get("baseline", dict()).get("custom_gems", False):
            # Ensure the custom Gemfile exists in the buildstock dir
            local_gemfile_path = os.path.join(self.buildstock_dir, "resources", "Gemfile")
            if not os.path.exists(local_gemfile_path):
                raise AttributeError(f"baseline:custom_gems = True, but did not find Gemfile at {local_gemfile_path}")

            # Copy the custom Gemfile into the buildstockbatch repo
            bsb_root = os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, os.pardir)
            new_gemfile_path = os.path.join(bsb_root, "Gemfile")
            shutil.copyfile(local_gemfile_path, new_gemfile_path)
            logger.info(f"Copying custom Gemfile from {local_gemfile_path}")

            # Choose the custom-gems stage in the Dockerfile,
            # which runs bundle install to build custom gems into the image
            stage = "buildstockbatch-custom-gems"
        else:
            # Choose the base stage in the Dockerfile,
            # which stops before bundling custom gems into the image
            stage = "buildstockbatch"

        logger.info(f"Building docker image stage: {stage} from OpenStudio {self.os_version}")
        img, build_logs = self.docker_client.images.build(
            path=str(root_path),
            tag=self.docker_image,
            rm=True,
            target=stage,
            platform="linux/amd64",
            buildargs={"OS_VER": self.os_version},
        )
        build_image_log = os.path.join(local_log_dir, "build_image.log")
        with open(build_image_log, "w") as f_out:
            f_out.write("Built image")
            for line in build_logs:
                for itm_type, item_msg in line.items():
                    if itm_type in ["stream", "status"]:
                        try:
                            f_out.write(f"{item_msg}")
                        except UnicodeEncodeError:
                            pass
        logger.debug(f"Review docker image build log: {build_image_log}")

        # Report and confirm the openstudio version from the image
        os_ver_cmd = "openstudio openstudio_version"
        container_output = self.docker_client.containers.run(
            self.docker_image, os_ver_cmd, remove=True, name="list_openstudio_version"
        )
        assert self.os_version in container_output.decode()

        # Report gems included in the docker image.
        # The OpenStudio Docker image installs the default gems
        # to /var/oscli/gems, and the custom docker image
        # overwrites these with the custom gems.
        list_gems_cmd = (
            "openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems "
            "--bundle_without native_ext gem_list"
        )
        container_output = self.docker_client.containers.run(
            self.docker_image, list_gems_cmd, remove=True, name="list_gems"
        )
        gem_list_log = os.path.join(local_log_dir, "openstudio_gem_list_output.log")
        with open(gem_list_log, "wb") as f_out:
            f_out.write(container_output)
        for line in container_output.decode().split("\n"):
            logger.debug(line)
        logger.debug(f"Review custom gems list at: {gem_list_log}")

    def push_image(self):
        """
        Push the locally built docker image to the GCP Artifact Repository (AR).
        """

        # Log the Docker client into the GCP AR registry
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.info("Using GOOGLE_APPLICATION_CREDENTIALS to authenticate Docker with Artifact Registry")
            service_account_key_file = open(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], "r")
            service_account_key = service_account_key_file.read()
            docker_client_login_response = self.docker_client.login(
                username="_json_key", password=service_account_key, registry=self.registry_url
            )
            logger.debug(docker_client_login_response)
        else:
            # Instructions for setting up these credentials are here:
            # https://cloud.google.com/artifact-registry/docs/docker/authentication#gcloud-helper
            logger.info(
                "Using Artifact Registry credentials from gcloud because GOOGLE_APPLICATION_CREDENTIALS is not set."
            )

        # Tag the image with a repo name for pushing to GCP AR
        image = self.docker_client.images.get(self.docker_image)
        repo_uri = self.repository_uri
        image.tag(repo_uri, tag=self.job_identifier)

        # Push to the GCP AR
        last_status = None
        for x in self.docker_client.images.push(repo_uri, tag=self.job_identifier, stream=True):
            try:
                y = json.loads(x)
            except json.JSONDecodeError:
                continue
            else:
                if y.get("status") is not None and y.get("status") != last_status:
                    logger.debug(y["status"])
                    last_status = y["status"]

    @property
    def gcp_batch_parent(self):
        return f"projects/{self.gcp_project}/locations/{self.region}"

    @property
    def gcp_batch_job_name(self):
        return f"{self.gcp_batch_parent}/jobs/{self.job_identifier}"

    def clean(self):
        delete_job(self.gcp_batch_job_name)
        self.clean_postprocessing_job()

        # Clean up images in Artifact Registry
        ar_client = artifactregistry_v1.ArtifactRegistryClient()
        repo_name = self.get_AR_repo_name(self.gcp_project, self.region, self.ar_repo)
        package = f"{repo_name}/packages/buildstockbatch"
        # Delete the tag used by this job
        try:
            ar_client.delete_tag(name=f"{package}/tags/{self.job_identifier}")
        except exceptions.NotFound:
            logger.debug(f"No `{self.job_identifier}` tag found in Aritfact Registry")

        # Then delete all untagged versions
        all_versions = ar_client.list_versions(
            artifactregistry_v1.ListVersionsRequest(parent=package, view=artifactregistry_v1.VersionView.FULL)
        )
        deleted = 0
        for version in all_versions:
            if not version.related_tags:
                logger.debug(f"Deleting image {version.name}")
                ar_client.delete_version(name=version.name)
                deleted += 1
        logger.info(f"Cleaned up {deleted} old docker images")

    def show_jobs(self):
        """
        Show the existing GCP Batch and Cloud Run jobs that match the provided project, if they exist.
        """
        # GCP Batch job that runs the simulations
        if job := self.get_existing_batch_job():
            logger.info("Batch job")
            logger.info(f"  Name: {job.name}")
            logger.info(f"  UID: {job.uid}")
            logger.info(f"  Status: {job.status.state.name}")
            task_counts = collections.defaultdict(int)
            for group in job.status.task_groups.values():
                for status, count in group.counts.items():
                    task_counts[status] += count
            logger.info(f"  Task statuses: {dict(task_counts)}")
            logger.debug(f"Full job info:\n{job}")
        else:
            logger.info(f"No existing Batch jobs match: {self.gcp_batch_job_name}")
        logger.info(f"See all Batch jobs at https://console.cloud.google.com/batch/jobs?project={self.gcp_project}")

        # Postprocessing Cloud Run job
        jobs_client = run_v2.JobsClient()
        try:
            job = jobs_client.get_job(name=self.postprocessing_job_name)
            last_execution = job.latest_created_execution
            status = "Running"
            if last_execution.completion_time:
                status = "Completed"
            logger.info("Post-processing Cloud Run job")
            logger.info(f"  Name: {job.name}")
            logger.info(f"  Status of latest run ({last_execution.name}): {status}")
            logger.debug(f"Full job info:\n{job}")
        except exceptions.NotFound:
            logger.info(f"No existing Cloud Run jobs match {self.postprocessing_job_name}")
        logger.info(f"See all Cloud Run jobs at https://console.cloud.google.com/run/jobs?project={self.gcp_project}")

    def get_existing_batch_job(self):
        client = batch_v1.BatchServiceClient()
        try:
            job = client.get_job(batch_v1.GetJobRequest(name=self.gcp_batch_job_name))
            return job
        except exceptions.NotFound:
            return None

    def get_existing_postprocessing_job(self):
        jobs_client = run_v2.JobsClient()
        try:
            job = jobs_client.get_job(name=self.postprocessing_job_name)
            return job
        except exceptions.NotFound:
            return False

    def check_for_existing_jobs(self, pp_only=False):
        """If there are existing jobs with the same ID as this job, logs them as errors and returns True.

        Checks for both the Batch job and Cloud Run post-processing job.

        :param pp_only: If true, only check for the post-processing job.
        """
        if pp_only:
            existing_batch_job = None
        elif existing_batch_job := self.get_existing_batch_job():
            logger.error(
                f"A Batch job with this ID ({self.job_identifier}) already exists "
                f"(status: {existing_batch_job.status.state.name}). Choose a new job_identifier or run with "
                "--clean to delete the existing job."
            )

        if existing_pp_job := self.get_existing_postprocessing_job():
            status = "Completed" if existing_pp_job.latest_created_execution.completion_time else "Running"
            logger.error(
                f"A Cloud Run job with this ID ({self.postprocessing_job_id}) already exists "
                f"(status: {status}). Choose a new job_identifier or run with --clean "
                "to delete the existing job."
            )
        return bool(existing_batch_job or existing_pp_job)

    def upload_batch_files_to_cloud(self, tmppath):
        """Implements :func:`DockerBase.upload_batch_files_to_cloud`"""
        logger.info("Uploading Batch files to Cloud Storage")
        upload_directory_to_GCS(tmppath, self.gcs_bucket, self.gcs_prefix + "/")

    def copy_files_at_cloud(self, files_to_copy):
        """Implements :func:`DockerBase.copy_files_at_cloud`"""
        logger.info("Copying weather files at Cloud Storage")
        Parallel(n_jobs=-1, verbose=9)(
            delayed(copy_GCS_file)(
                self.gcs_bucket,
                f"{self.gcs_prefix}/weather/{src}",
                self.gcs_bucket,
                f"{self.gcs_prefix}/weather/{dest}",
            )
            for src, dest in files_to_copy
        )

    def start_batch_job(self, batch_info):
        """Implements :func:`DockerBase.start_batch_job`"""
        # Define and run the GCP Batch job.
        logger.info("Setting up GCP Batch job")
        client = batch_v1.BatchServiceClient()

        runnable = batch_v1.Runnable()
        runnable.container = batch_v1.Runnable.Container()
        runnable.container.image_uri = self.repository_uri + ":" + self.job_identifier
        runnable.container.entrypoint = "/bin/sh"

        # Pass environment variables to each task
        environment = batch_v1.Environment()
        # BATCH_TASK_INDEX and BATCH_TASK_COUNT env vars are automatically made available by GCP Batch.
        environment.variables = {
            "JOB_NAME": self.job_identifier,
            "GCS_PREFIX": self.gcs_prefix,
            "GCS_BUCKET": self.gcs_bucket,
        }
        runnable.environment = environment

        runnable.container.commands = ["-c", "python3 -m buildstockbatch.gcp.gcp"]

        gcp_cfg = self.cfg["gcp"]
        job_env_cfg = gcp_cfg.get("job_environment", {})
        resources = batch_v1.ComputeResource(
            cpu_milli=1000 * job_env_cfg.get("vcpus", 1),
            memory_mib=job_env_cfg.get("memory_mib", 1024),
        )

        # Give three minutes per simulation, plus ten minutes for job overhead
        task_duration_secs = 60 * (10 + batch_info.n_sims_per_job * 3)
        task = batch_v1.TaskSpec(
            runnables=[runnable],
            compute_resource=resources,
            # Allow retries, but only when the machine is preempted.
            max_retry_count=3,
            lifecycle_policies=[
                batch_v1.LifecyclePolicy(
                    action=batch_v1.LifecyclePolicy.Action.RETRY_TASK,
                    action_condition=batch_v1.LifecyclePolicy.ActionCondition(
                        exit_codes=[50001]  # Exit code for preemptions
                    ),
                )
            ],
            max_run_duration=f"{task_duration_secs}s",
        )

        # How many of these tasks to run.
        group = batch_v1.TaskGroup(
            task_count=batch_info.job_count,
            task_spec=task,
        )

        # Specify type of VMs to run on
        policy = batch_v1.AllocationPolicy.InstancePolicy(
            # If machine type isn't specified, GCP Batch will choose a type based on the resources requested.
            machine_type=job_env_cfg.get("machine_type"),
            provisioning_model=(
                batch_v1.AllocationPolicy.ProvisioningModel.SPOT
                if job_env_cfg.get("use_spot")
                else batch_v1.AllocationPolicy.ProvisioningModel.STANDARD
            ),
        )
        instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=policy)
        allocation_policy = batch_v1.AllocationPolicy(instances=[instances])
        if service_account := gcp_cfg.get("service_account"):
            allocation_policy.service_account = batch_v1.ServiceAccount(email=service_account)

        # Define the batch job
        job = batch_v1.Job()
        job.task_groups = [group]
        job.allocation_policy = allocation_policy
        job.logs_policy = batch_v1.LogsPolicy()
        job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

        create_request = batch_v1.CreateJobRequest()
        create_request.job = job
        create_request.job_id = self.job_identifier
        create_request.parent = f"projects/{self.gcp_project}/locations/{self.region}"

        # Start the job!
        created_job = client.create_job(create_request)
        job_name = created_job.name

        logger.info("Newly created GCP Batch job")
        logger.info(f"  Job name: {job_name}")
        logger.info(f"  Job UID: {created_job.uid}")
        job_url = (
            "https://console.cloud.google.com/batch/jobsDetail/regions/"
            f"{self.region}/jobs/{self.job_identifier}/details?project={self.gcp_project}"
        )
        logger.info(f"View GCP Batch job at {job_url}")

        # Monitor job status while waiting for the job to complete
        n_completed_last_time = 0
        client = batch_v1.BatchServiceClient()
        with tqdm.tqdm(desc="Running Simulations", total=batch_info.job_count, unit="batch") as progress_bar:
            job_status = None
            while job_status not in ("SUCCEEDED", "FAILED", "DELETION_IN_PROGRESS"):
                time.sleep(10)
                job_info = client.get_job(batch_v1.GetJobRequest(name=job_name))
                job_status = job_info.status.state.name
                # Check how many tasks have succeeded
                task_counts = collections.defaultdict(int)
                for group in job_info.status.task_groups.values():
                    for status, count in group.counts.items():
                        task_counts[status] += count
                n_completed = task_counts.get("SUCCEEDED", 0) + task_counts.get("FAILED", 0)
                progress_bar.update(n_completed - n_completed_last_time)
                n_completed_last_time = n_completed
                # Show all task status counts next to the progress bar
                progress_bar.set_postfix_str(f"{dict(task_counts)}")

        logger.info(f"Batch job status: {job_status}")
        if job_status != "SUCCEEDED":
            raise RuntimeError(f"Batch job failed. See GCP logs at {job_url}")
        else:
            task_group = job_info.task_groups[0]
            task_spec = task_group.task_spec
            instance = job_info.status.task_groups["group0"].instances[0]

            # Output stats in spreadsheet-friendly format
            tsv_logger = TsvLogger()
            tsv_logger.append_stat("Simulations", batch_info.n_sims)
            tsv_logger.append_stat("Tasks", task_group.task_count)
            tsv_logger.append_stat("Parallelism", task_group.parallelism)
            tsv_logger.append_stat("mCPU/task", task_spec.compute_resource.cpu_milli)
            tsv_logger.append_stat("MiB/task", task_spec.compute_resource.memory_mib)
            tsv_logger.append_stat("Machine type", instance.machine_type)
            tsv_logger.append_stat("Provisioning", instance.provisioning_model.name)
            tsv_logger.append_stat("Runtime", job_info.status.run_duration)
            tsv_logger.log_stats(logging.INFO)

    @classmethod
    def run_task(cls, task_index, job_name, gcs_bucket, gcs_prefix):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in GCP compute engine.
        It will read the necessary files from GCS, run the simulation, and write the
        results back to GCS.

        :param task_index: Index of this task (e.g. this may be task 1 of 4)
        :param job_name: Job identifier
        :param gcs_bucket: GCS bucket for input and output files
        :param gcs_prefix: Prefix used for GCS files
        """
        # Local directory where we'll write files
        sim_dir = pathlib.Path("/var/simdata/openstudio")

        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket)

        logger.info("Extracting assets TAR file")
        # Copy assets file to local machine to extract TAR file
        assets_file_path = sim_dir.parent / "assets.tar.gz"
        bucket.blob(f"{gcs_prefix}/assets.tar.gz").download_to_filename(assets_file_path)
        with tarfile.open(assets_file_path, "r") as tar_f:
            tar_f.extractall(sim_dir)

        logger.debug("Reading config")
        blob = bucket.blob(f"{gcs_prefix}/config.json")
        cfg = json.loads(blob.download_as_string())

        # Extract the job information for this particular task
        logger.debug("Getting job information")
        jobs_file_path = sim_dir.parent / "jobs.tar.gz"
        bucket.blob(f"{gcs_prefix}/jobs.tar.gz").download_to_filename(jobs_file_path)
        with tarfile.open(jobs_file_path, "r") as tar_f:
            jobs_d = json.load(tar_f.extractfile(f"jobs/job{task_index:05d}.json"), encoding="utf-8")
        logger.debug("Number of simulations = {}".format(len(jobs_d["batch"])))

        logger.debug("Getting weather files")
        weather_dir = sim_dir / "weather"
        os.makedirs(weather_dir, exist_ok=True)

        epws_to_download = docker_base.determine_epws_needed_for_job(sim_dir, jobs_d)

        # Download and unzip the epws needed for these simulations
        for epw_filename in epws_to_download:
            epw_filename = os.path.basename(epw_filename)
            with io.BytesIO() as f_gz:
                logger.debug("Downloading {}.gz".format(epw_filename))
                bucket.blob(f"{gcs_prefix}/weather/{epw_filename}.gz").download_to_file(f_gz)
                with open(weather_dir / epw_filename, "wb") as f_out:
                    logger.debug("Extracting {}".format(epw_filename))
                    f_out.write(gzip.decompress(f_gz.getvalue()))

        cls.run_simulations(cfg, task_index, jobs_d, sim_dir, GCSFileSystem(), f"{gcs_bucket}/{gcs_prefix}")

    # todo: Do cleanup (which the aws script does, in the 'nrel/aws_batch' branch)
    # todo: aws-shared (see file comment): Such cleanup should be shared with the aws script.
    def cleanup_dask(self):
        pass

    def get_fs(self):
        """
        Overrides `BuildStockBatchBase.get_fs()` (in the 'nrel/aws_batch' branch). This would
        indirectly result in `postprocessing.combine_results()` writing to GCS (GCP Cloud Storage);
        however, we also override `BuildStockBatch.process_results()`, so we also make the call to
        `postprocessing.combine_results()` and can directly define where that writes to.

        :returns: A `GCSFileSystem()`.
        """
        return GCSFileSystem()

    def process_results(self, skip_combine=False, use_dask_cluster=True):
        """
        Overrides `BuildStockBatchBase.process_results()`.

        While the BuildStockBatchBase implementation uploads to S3, this uploads to GCP Cloud
        Storage. The BSB implementation tries to write both indirectly (via
        `postprocessing.combine_results()`, using `get_fs()`), and directly (through
        `upload_results`). Which way the results end up on S3 depends on whether the script was run
        via aws.py (indirect write), or locally or Eagle (direct upload).

        Here, where writing to GCS is (currently) coupled to running on GCS, the writing
        to GCS will happen indirectly (via `postprocessing.combine_results()`), and we don't need to
        also try to explicitly upload results.

        TODO: `use_dask_cluster` (which comes from the parent implementation) is ignored. The job,
        run on Cloud Run, always uses Dask, in part because `postprocessing.combine_results` fails
        if `DaskClient()` is not initialized. Once `combine_results` is fixed to work without
        DaskClient, the `use_dask_cluster` param needs to be piped through environment variables to
        `run_combine_results_on_cloud`.
        """

        wfg_args = self.cfg["workflow_generator"].get("args", {})
        if self.cfg["workflow_generator"]["type"] == "residential_hpxml":
            if "simulation_output_report" in wfg_args.keys():
                if "timeseries_frequency" in wfg_args["simulation_output_report"].keys():
                    do_timeseries = wfg_args["simulation_output_report"]["timeseries_frequency"] != "none"
        else:
            do_timeseries = "timeseries_csv_export" in wfg_args.keys()

        if not skip_combine:
            self.start_combine_results_job_on_cloud(self.results_dir, do_timeseries=do_timeseries)

    @classmethod
    def run_combine_results_on_cloud(cls, gcs_bucket, gcs_prefix, results_dir, do_timeseries):
        """This is the function that is run on the cloud to actually perform `combine_results` on
        the cloud.
        """
        logger.info("run_combine_results_on_cloud starting")
        client = storage.Client()
        bucket = client.get_bucket(gcs_bucket)

        logger.debug("Reading config")
        blob = bucket.blob(f"{gcs_prefix}/config.json")
        cfg = json.loads(blob.download_as_string())

        DaskClient()
        postprocessing.combine_results(GCSFileSystem(), results_dir, cfg, do_timeseries=do_timeseries)

    def start_combine_results_job_on_cloud(self, results_dir, do_timeseries=True):
        """Set up `combine_results` to be run on GCP Cloud Run.

        Parameters are passed to `combine_results` (so see that for parameter documentation).
        """
        logger.info("Creating job to run combine_results on Cloud Run...")

        # Define the Job
        pp_env_cfg = self.cfg["gcp"].get("postprocessing_environment", {})
        memory_mib = pp_env_cfg.get("memory_mib", self.DEFAULT_PP_MEMORY_MIB)
        cpus = pp_env_cfg.get("cpus", self.DEFAULT_PP_CPUS)
        job = run_v2.Job(
            template=run_v2.ExecutionTemplate(
                template=run_v2.TaskTemplate(
                    containers=[
                        run_v2.Container(
                            name=self.job_identifier,
                            image=self.repository_uri + ":" + self.job_identifier,
                            resources=run_v2.ResourceRequirements(
                                limits={
                                    "memory": f"{memory_mib}Mi",
                                    "cpu": str(cpus),
                                }
                            ),
                            command=["/bin/sh"],
                            args=["-c", "python3 -m buildstockbatch.gcp.gcp"],
                            env=[
                                run_v2.EnvVar(name="JOB_TYPE", value="POSTPROCESS"),
                                run_v2.EnvVar(name="GCS_PREFIX", value=self.gcs_prefix),
                                run_v2.EnvVar(name="GCS_BUCKET", value=self.gcs_bucket),
                                run_v2.EnvVar(name="RESULTS_DIR", value=results_dir),
                                run_v2.EnvVar(name="DO_TIMESERIES", value="True" if do_timeseries else "False"),
                            ],
                        )
                    ],
                    timeout=f"{60 * 60 * 24}s",  # 24h
                    max_retries=0,
                    service_account=self.cfg["gcp"].get("service_account"),
                )
            )
        )

        # Create the job
        jobs_client = run_v2.JobsClient()
        jobs_client.create_job(
            run_v2.CreateJobRequest(
                parent=f"projects/{self.gcp_project}/locations/{self.region}",
                job_id=self.postprocessing_job_id,
                job=job,
            )
        )

        # Start the job!
        attempts_remaining = 3
        while True:
            try:
                jobs_client.run_job(name=self.postprocessing_job_name)
                logger.info(
                    f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║ Post-processing Cloud Run Job started!                                       ║
║                                                                              ║
║ You may interrupt the script and the job will continue to run.               ║
╚══════════════════════════════════════════════════════════════════════════════╝

🔗 See status at: {self.postprocessing_job_console_url}.

Run this script with --clean to clean up the GCP environment after post-processing is complete."""
                )
                break
            except:
                attempts_remaining -= 1
                if attempts_remaining > 0:
                    # retry after delay
                    logger.warning(
                        "Post-processing Cloud Run job failed to start. "
                        f"{attempts_remaining} attempt(s) remaining. "
                        "Will retry in 1 second...",
                        exc_info=logger.isEnabledFor(logging.DEBUG),
                    )
                    time.sleep(1)
                    continue

                # no attempts remaining
                logger.warning(
                    "Post-processing Cloud Run job failed to start after three attempts. "
                    "You may want to investigate why and try starting it at the console: "
                    f"{self.postprocessing_job_console_url}",
                    exc_info=True,
                )
                return

        # Monitor job/execution status, starting by polling the Job for an Execution
        logger.info("Waiting for execution to begin...")
        job_start_time = datetime.now()
        job = self.get_existing_postprocessing_job()
        while not job.latest_created_execution:
            time.sleep(1)
            job = self.get_existing_postprocessing_job()
        execution_start_time = datetime.now()
        logger.info(
            f"Execution has started (after {str(execution_start_time - job_start_time)} "
            "seconds). Waiting for execution to finish..."
        )

        # Have an execution; poll that for completion
        fail_message = None
        with tqdm.tqdm(
            desc="Waiting for post-processing execution to complete", bar_format="{desc}: {elapsed}{postfix}"
        ) as pp_tqdm:
            spinner_states = ["|", "/", "-", "\\"]
            spinner_state = 0

            pp_tqdm.set_postfix_str("|")
            executions_client = run_v2.ExecutionsClient()
            execution_name = f"{self.postprocessing_job_name}/executions/{job.latest_created_execution.name}"
            last_query_time = time.time()
            while True:
                # update spinner frequently...
                time.sleep(0.25)
                # ...but only actually query status every 10 sec
                if time.time() - last_query_time > 10:
                    # fetch and extract the status
                    execution = executions_client.get_execution(name=execution_name)
                    last_query_time = time.time()

                    if execution.succeeded_count > 0:
                        # Done!
                        break
                    elif execution.failed_count > 0:
                        fail_message = "🟥 Post-processing execution failed."
                        break
                    elif execution.cancelled_count > 0:
                        fail_message = "🟧 Post-processing execution canceled."
                        break

                spinner_state = (spinner_state + 1) % len(spinner_states)
                pp_tqdm.set_postfix_str(spinner_states[spinner_state])
                pp_tqdm.update()

        if fail_message is not None:
            # if logged within the tqdm block, the message ends up on the same line as the status
            logger.warning(f"{fail_message} See {self.postprocessing_job_console_url} for more information")
            return

        logger.info(f"🟢 Post-processing finished! ({str(datetime.now() - execution_start_time)}). ")

        # Output stats in spreadsheet-friendly format
        # completion_time might not be set right away; if not, just use current time (close enough)
        finish_time = execution.completion_time if execution.completion_time is not None else datetime.now()
        tsv_logger = TsvLogger()
        tsv_logger.append_stat("cpus", cpus)
        tsv_logger.append_stat("memory_mib", memory_mib)
        tsv_logger.append_stat("Succeeded", "Yes")
        tsv_logger.append_stat("Job Created", job.create_time.strftime("%H:%M:%S"))
        tsv_logger.append_stat("Exec Start", execution.start_time.strftime("%H:%M:%S"))
        tsv_logger.append_stat("Script Start", "?")
        tsv_logger.append_stat("Exec Finish", finish_time.strftime("%H:%M:%S"))
        tsv_logger.log_stats(logging.INFO)

    def clean_postprocessing_job(self):
        logger.info(
            "Cleaning post-processing Cloud Run job with "
            f"job_identifier='{self.job_identifier}'; "
            f"job name={self.postprocessing_job_name}..."
        )
        job = self.get_existing_postprocessing_job()
        if not job:
            logger.warning(
                "Post-processing Cloud Run job not found for "
                f"job_identifier='{self.job_identifier}' "
                f"(postprocessing_job_name='{self.postprocessing_job_name}')."
            )
            return

        # Ask for confirmation to delete if it is not completed
        if int(job.latest_created_execution.completion_time.timestamp()) == 0:
            answer = input(
                "Post-processing job does not appear to be completed. "
                "Are you sure you want to cancel and delete it? (y/n) "
            )
            if answer[:1] not in ("y", "Y"):
                return

            # Delete execution first
            executions_client = run_v2.ExecutionsClient()
            try:
                executions_client.cancel_execution(name=job.latest_created_execution.name)
            except Exception:
                logger.warning(
                    "Failed to cancel execution with name={job.latest_created_execution.name}.", exc_info=True
                )
                logger.warning(
                    f"You may want to try deleting the job via the console: {self.postprocessing_job_console_url}"
                )
            return

        # ... The job succeeded or its execution was deleted successfully; it can be deleted
        jobs_client = run_v2.JobsClient()
        try:
            jobs_client.delete_job(name=self.postprocessing_job_name)
        except Exception:
            logger.warning("Failed to deleted post-processing Cloud Run job.", exc_info=True)
        logger.info(f"Post-processing Cloud Run job deleted: '{self.postprocessing_job_name}'")

    def upload_results(self, *args, **kwargs):
        """
        Overrides `BuildStockBatchBase.upload_results()` from base.

        Does nothing — in fact, this override is not called and not necessary — because the results
        are already on GCS (`postprocessing.combine_results`, via `process_results()` here, wrote
        directly to GCS). But this is here in case `upload_results()` is called, and to match aws.py
        if/when we refactor to make GCS usable from other contexts (e.g., running locally).
        """
        return self.bucket, f"{self.prefix}/parquet"


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
                "__main__": {"level": "DEBUG", "propagate": True, "handlers": ["console"]},
                "buildstockbatch": {"level": "DEBUG", "propagate": True, "handlers": ["console"]},
            },
        }
    )
    print(GcpBatch.LOGO)
    if "BATCH_TASK_INDEX" in os.environ:
        # If this var exists, we're inside a single batch task.
        task_index = int(os.environ["BATCH_TASK_INDEX"])
        gcs_bucket = os.environ["GCS_BUCKET"]
        gcs_prefix = os.environ["GCS_PREFIX"]
        job_name = os.environ["JOB_NAME"]
        GcpBatch.run_task(task_index, job_name, gcs_bucket, gcs_prefix)
    elif "POSTPROCESS" == os.environ.get("JOB_TYPE", ""):
        gcs_bucket = os.environ["GCS_BUCKET"]
        gcs_prefix = os.environ["GCS_PREFIX"]
        results_dir = os.environ["RESULTS_DIR"]
        do_timeseries = os.environ.get("DO_TIMESERIES", "False") == "True"
        GcpBatch.run_combine_results_on_cloud(gcs_bucket, gcs_prefix, results_dir, do_timeseries)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument("project_filename")
        parser.add_argument(
            "job_identifier",
            nargs="?",
            default=None,
            help="Optional override of gcp.job_identifier in your project file. Max 48 characters.",
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-c",
            "--clean",
            action="store_true",
            help="After the simulation is done, run with --clean to clean up GCP environment. "
            "If the GCP Batch job is still running, this will cancel the job.",
        )
        group.add_argument(
            "--validateonly",
            help="Only validate the project YAML file and references. Nothing is executed",
            action="store_true",
        )
        group.add_argument("--show_jobs", help="List existing jobs", action="store_true")
        group.add_argument(
            "--postprocessonly",
            help="Only do postprocessing, useful for when the simulations are already done",
            action="store_true",
        )
        group.add_argument(
            "--crawl",
            help="Only do the crawling in Athena. When simulations and postprocessing are done.",
            action="store_true",
        )
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Verbose output - includes DEBUG logs if set",
        )
        args = parser.parse_args()

        if args.verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        # validate the project, and if --validateonly flag is set, return True if validation passes
        GcpBatch.validate_project(os.path.abspath(args.project_filename))
        if args.validateonly:
            return True

        batch = GcpBatch(args.project_filename, args.job_identifier)
        if args.clean:
            batch.clean()
            return
        if args.show_jobs:
            batch.show_jobs()
            return
        elif args.postprocessonly:
            if batch.check_for_existing_jobs(pp_only=True):
                return
            batch.build_image()
            batch.push_image()
            batch.process_results()
        elif args.crawl:
            batch.process_results(skip_combine=True, use_dask_cluster=False)
        else:
            if batch.check_for_existing_jobs():
                return

            batch.build_image()
            batch.push_image()
            batch.run_batch()
            batch.process_results()
            # process_results is async, so don't do a clean (which would clean before it's done)


if __name__ == "__main__":
    main()
