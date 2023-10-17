# -*- coding: utf-8 -*-

"""
buildstockbatch.gcp
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with GCP Batch.

This implementation tries to match the structure of `../aws/aws.py` in the 'aws-batch' branch as
much as possible in order to make it easier to refactor these two (or three, with Eagle) to share
code later. Also, because that branch has not yet been merged, this will also _not_ do any
refactoring right now to share code with that (to reduce merging complexity). Instead, code that's
likely to be refactored out will be commented with 'todo: aws-shared'.

:author: Robert LaThanh
:copyright: (c) 2023 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import argparse
import docker
import json
import logging
import os
import pathlib
import re

from buildstockbatch.base import ValidationError, BuildStockBatchBase
from buildstockbatch import postprocessing
from buildstockbatch.utils import ContainerRuntime, log_error_details, get_project_configuration

logger = logging.getLogger(__name__)

# todo: aws-shared (see file comment)
class DockerBatchBase(BuildStockBatchBase):

    CONTAINER_RUNTIME = ContainerRuntime.DOCKER

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.docker_client = docker.DockerClient.from_env()
        try:
            self.docker_client.ping()
        except:  # noqa: E722 (allow bare except in this case because error can be a weird non-class Windows API error)
            logger.error('The docker server did not respond, make sure Docker Desktop is started then retry.')
            raise RuntimeError('The docker server did not respond, make sure Docker Desktop is started then retry.')

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)

    @property
    def docker_image(self):
        return f"nrel/openstudio:{self.os_version}"

class GcpBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.job_identifier = re.sub('[^0-9a-zA-Z]+', '_', self.cfg['gcp']['job_identifier'])[:10]

        self.project_filename = project_filename
        self.region = self.cfg['gcp']['region']
        self.project = self.cfg['gcp']['project']
        self.ar_repo = self.cfg['gcp']['artifact_registry']['repository']


    @staticmethod
    def validate_project(project_file):
        return # todo-xxx- to be (re)implemented
        super(GcpBatch, GcpBatch).validate_project(project_file)
        GcpBatch.validate_dask_settings(project_file)

    @property
    def docker_image(self):
        return 'nrel/buildstockbatch'

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
        return f"{self.region}-docker.pkg.dev/{self.project}/{self.ar_repo}/buildstockbatch"

    # todo: aws-shared (see file comment)
    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / 'Dockerfile').exists():
            raise RuntimeError(f'The needs to be run from the root of the repo, found {root_path}')

        # Make the buildstock/resources/.gcp_docker_image dir to store logs
        local_log_dir = os.path.join(self.buildstock_dir, 'resources', '.gcp_docker_image')
        if not os.path.exists(local_log_dir):
            os.makedirs(local_log_dir)

        # Determine whether or not to build the image with custom gems bundled in
        if self.cfg.get('baseline', dict()).get('custom_gems', False):
            # Ensure the custom Gemfile exists in the buildstock dir
            local_gemfile_path = os.path.join(self.buildstock_dir, 'resources', 'Gemfile')
            if not os.path.exists(local_gemfile_path):
                raise AttributeError(f'baseline:custom_gems = True, but did not find Gemfile at {local_gemfile_path}')

            # Copy the custom Gemfile into the buildstockbatch repo
            bsb_root = os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, os.pardir)
            new_gemfile_path = os.path.join(bsb_root, 'Gemfile')
            shutil.copyfile(local_gemfile_path, new_gemfile_path)
            logger.info(f'Copying custom Gemfile from {local_gemfile_path}')

            # Choose the custom-gems stage in the Dockerfile,
            # which runs bundle install to build custom gems into the image
            stage = 'buildstockbatch-custom-gems'
        else:
            # Choose the base stage in the Dockerfile,
            # which stops before bundling custom gems into the image
            stage = 'buildstockbatch'

        logger.info(f'Building docker image stage: {stage} from OpenStudio {self.os_version}')
        img, build_logs = self.docker_client.images.build(
            path=str(root_path),
            tag=self.docker_image,
            rm=True,
            target=stage,
            platform="linux/amd64",
            buildargs={'OS_VER': self.os_version}
        )
        build_image_log = os.path.join(local_log_dir, 'build_image.log')
        with open(build_image_log, 'w') as f_out:
            f_out.write('Built image')
            for line in build_logs:
                for itm_type, item_msg in line.items():
                    if itm_type in ['stream', 'status']:
                        try:
                            f_out.write(f'{item_msg}')
                        except UnicodeEncodeError:
                            pass
        logger.debug(f'Review docker image build log: {build_image_log}')

        # Report and confirm the openstudio version from the image
        os_ver_cmd = 'openstudio openstudio_version'
        container_output = self.docker_client.containers.run(
            self.docker_image,
            os_ver_cmd,
            remove=True,
            name='list_openstudio_version'
        )
        assert self.os_version in container_output.decode()

        # Report gems included in the docker image.
        # The OpenStudio Docker image installs the default gems
        # to /var/oscli/gems, and the custom docker image
        # overwrites these with the custom gems.
        list_gems_cmd = 'openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems ' \
                        '--bundle_without native_ext gem_list'
        container_output = self.docker_client.containers.run(
            self.docker_image,
            list_gems_cmd,
            remove=True,
            name='list_gems'
        )
        gem_list_log = os.path.join(local_log_dir, 'openstudio_gem_list_output.log')
        with open(gem_list_log, 'wb') as f_out:
            f_out.write(container_output)
        for line in container_output.decode().split('\n'):
            logger.debug(line)
        logger.debug(f'Review custom gems list at: {gem_list_log}')

    def push_image(self):
        """
        Push the locally built docker image to the GCP Artifact Repository (AR).
        """

        # Log the Docker client into the GCP AR registry
        service_account_key_file = open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "r")
        service_account_key = service_account_key_file.read()
        docker_client_login_response = self.docker_client.login(
            username="_json_key",
            password=service_account_key,
            registry=self.registry_url
        )
        logger.debug(docker_client_login_response)

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
                if y.get('status') is not None and y.get('status') != last_status:
                    logger.debug(y['status'])
                    last_status = y['status']


@log_error_details()
def main():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'defaultfmt': {
                'format': '%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'defaultfmt',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            }
        },
        'loggers': {
            '__main__': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            },
            'buildstockbatch': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            },
        },
    })
    print(GcpBatch.LOGO)
    if 'GCP_BATCH_JOB_ARRAY_INDEX' in os.environ:
        job_id = int(os.environ['GCP_BATCH_JOB_ARRAY_INDEX'])
        gcs_bucket = os.environ['GCS_BUCKET']
        gcs_prefix = os.environ['GCS_PREFIX']
        job_name = os.environ['JOB_NAME']
        region = os.environ['REGION']
        GcpBatch.run_job(job_id, gcs_bucket, gcs_prefix, job_name, region)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('project_filename')
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            '-c', '--clean',
            action='store_true',
            help='After the simulation is done, run with --clean to clean up GCP environment'
        )
        group.add_argument(
            '--validateonly',
            help='Only validate the project YAML file and references. Nothing is executed',
            action='store_true'
        )
        group.add_argument(
            '--postprocessonly',
            help='Only do postprocessing, useful for when the simulations are already done',
            action='store_true'
        )
        group.add_argument(
            '--crawl',
            help='Only do the crawling in Athena. When simulations and postprocessing are done.',
            action='store_true'
        )
        args = parser.parse_args()

        # validate the project, and in case of the --validateonly flag return True if validation passes
        GcpBatch.validate_project(os.path.abspath(args.project_filename))
        if args.validateonly:
            return True

        batch = GcpBatch(args.project_filename)
        if args.clean:
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            # batch.clean()
        elif args.postprocessonly:
            batch.build_image()
            batch.push_image()
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.process_results()
        elif args.crawl:
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.process_results(skip_combine=True, use_dask_cluster=False)
        else:
            batch.build_image()
            batch.push_image()
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.run_batch()
            batch.process_results()
            batch.clean()


if __name__ == '__main__':
    main()
