# -*- coding: utf-8 -*-

"""
buildstockbatch.aws
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with AWS Batch

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import argparse
import base64
import boto3
import gzip
import io
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pandas as pd
import pathlib
import random
import shutil
import subprocess
import tarfile
import tempfile

from buildstockbatch.localdocker import DockerBatchBase
from buildstockbatch.base import (
    read_data_point_out_json,
    to_camelcase,
    flatten_datapoint_json,
    read_out_osw
)

logger = logging.getLogger(__name__)


def upload_file_to_s3(*args, **kwargs):
    s3 = boto3.client('s3')
    s3.upload_file(*args, **kwargs)


def upload_directory_to_s3(local_directory, bucket, prefix):
    local_dir_abs = pathlib.Path(local_directory).absolute()

    def filename_generator():
        for dirpath, dirnames, filenames in os.walk(local_dir_abs):
            for filename in filenames:
                if filename.startswith('.'):
                    continue
                local_filepath = pathlib.Path(dirpath, filename)
                s3_key = pathlib.PurePosixPath(
                    prefix,
                    local_filepath.relative_to(local_dir_abs)
                )
                yield local_filepath, s3_key

    logger.debug('Uploading {} => {}/{}'.format(local_dir_abs, bucket, prefix))

    Parallel(n_jobs=-1, verbose=9)(
        delayed(upload_file_to_s3)(str(local_file), bucket, s3_key.as_posix())
        for local_file, s3_key
        in filename_generator()
    )


def compress_file(in_filename, out_filename):
    with gzip.open(str(out_filename), 'wb') as f_out:
        with open(str(in_filename), 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)


class AwsBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.ecr = boto3.client('ecr')
        self.s3 = boto3.client('s3')

    @classmethod
    def docker_image(cls):
        return 'nrel/buildstockbatch'

    @property
    def container_repo(self):
        repo_name = self.docker_image()
        repos = self.ecr.describe_repositories()
        repo = None
        for repo in repos['repositories']:
            if repo['repositoryName'] == repo_name:
                break
        if repo is None:
            resp = self.ecr.create_repository(repositoryName=repo_name)
            repo = resp['repository']
        return repo

    def push_image(self):
        """
        Push the locally built docker image to the AWS docker repo
        """
        auth_token = self.ecr.get_authorization_token()
        dkr_user, dkr_pass = base64.b64decode(auth_token['authorizationData'][0]['authorizationToken']).\
            decode('ascii').split(':')
        repo_url = self.container_repo['repositoryUri']
        registry_url = 'https://' + repo_url.split('/')[0]
        resp = self.docker_client.login(
            username=dkr_user,
            password=dkr_pass,
            registry=registry_url
        )
        logger.debug(resp)
        image = self.docker_client.images.get(self.docker_image())
        image.tag(repo_url)
        last_status = None
        for x in self.docker_client.images.push(repo_url, stream=True):
            try:
                y = json.loads(x)
            except json.JSONDecodeError:
                continue
            else:
                if y.get('status') is not None and y.get('status') != last_status:
                    logger.debug(y['status'])
                    last_status = y['status']

    def run_batch(self):
        """
        Run a batch of simulations using AWS Batch

        This will
            - perform the sampling
            - package and upload the assets, including weather
            - kick off a batch simulation on AWS
        """

        # Generate buildstock.csv
        if 'downselect' in self.cfg:
            buildstock_csv_filename = self.downselect()
        else:
            buildstock_csv_filename = self.run_sampling()

        # Compress and upload assets to S3
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = pathlib.Path(tmpdir)
            with tarfile.open(tmppath / 'assets.tar.gz', 'x:gz') as tar_f:
                project_path = pathlib.Path(self.project_dir)
                buildstock_path = pathlib.Path(self.buildstock_dir)
                tar_f.add(buildstock_path / 'measures', 'measures')
                tar_f.add(buildstock_path / 'resources', 'lib/resources')
                tar_f.add(project_path / 'housing_characteristics', 'lib/housing_characteristics')
                tar_f.add(project_path / 'seeds', 'seeds')
                tar_f.add(project_path / 'weather', 'weather')
            weather_path = tmppath / 'weather'
            os.makedirs(weather_path)
            Parallel(n_jobs=-1, verbose=9)(
                delayed(compress_file)(
                    pathlib.Path(self.weather_dir) / epw_filename,
                    str(weather_path / epw_filename) + '.gz'
                )
                for epw_filename
                in filter(lambda x: x.endswith('.epw'), os.listdir(self.weather_dir))
            )
            with open(tmppath / 'config.json', 'wt', encoding='utf-8') as f:
                json.dump(self.cfg, f)

            # Collect simulations to queue
            df = pd.read_csv(buildstock_csv_filename, index_col=0)
            building_ids = df.index.tolist()
            n_datapoints = len(building_ids)
            n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)

            # This is the maximum number of jobs that can be in an array
            n_jobs = 10000
            n_sims_per_job = math.ceil(n_sims / n_jobs)
            n_sims_per_job = max(n_sims_per_job, 2)

            baseline_sims = zip(building_ids, itertools.repeat(None))
            upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get('upgrades', []))))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
            random.shuffle(all_sims)
            all_sims_iter = iter(all_sims)

            os.makedirs(tmppath / 'jobs')

            for i in itertools.count(0):
                batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
                if not batch:
                    break
                logging.info('Queueing job {} ({} simulations)'.format(i, len(batch)))
                job_json_filename = tmppath / 'jobs' / 'job{:05d}.json'.format(i)
                with open(job_json_filename, 'w') as f:
                    json.dump({
                        'job_num': i,
                        'batch': batch,
                    }, f, indent=4)

            upload_directory_to_s3(
                tmppath,
                self.cfg['aws']['s3']['bucket'],
                self.cfg['aws']['s3']['prefix']
            )

        # TODO: Setup Compute Environment, Job queue, IAM Roles, Job Defn, Start Job

    @classmethod
    def run_job(cls, job_id, bucket, prefix):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in AWS. It will
        go get the necessary files from S3, run the simulation, and post the
        results back to S3.
        """
        s3 = boto3.client('s3')
        sim_dir = pathlib.Path('/var/simdata/openstudio')

        logger.debug('Downloading assets')
        assets_file_path = sim_dir.parent / 'assets.tar.gz'
        s3.download_file(bucket, '{}/assets.tar.gz'.format(prefix), str(assets_file_path))
        with tarfile.open(assets_file_path, 'r') as tar_f:
            tar_f.extractall(sim_dir)
        os.remove(assets_file_path)
        asset_dirs = os.listdir(sim_dir)

        logger.debug('Reading config')
        with io.BytesIO() as f:
            s3.download_fileobj(bucket, '{}/config.json'.format(prefix), f)
            cfg = json.loads(f.getvalue(), encoding='utf-8')

        logger.debug('Getting job information')
        with io.BytesIO() as f:
            s3.download_fileobj(bucket, '{}/jobs/job{:05d}.json'.format(prefix, job_id), f)
            jobs_d = json.loads(f.getvalue(), encoding='utf-8')
        logger.debug('Number of simulations = {}'.format(len(jobs_d['batch'])))

        logger.debug('Getting weather files')
        df = pd.read_csv(str(sim_dir / 'lib' / 'housing_characteristics' / 'buildstock.csv'), index_col=0)
        epws_to_download = df.loc[[x[0] for x in jobs_d['batch']], 'Location EPW'].unique().tolist()
        for epw_filename in epws_to_download:
            with io.BytesIO() as f_gz:
                logger.debug('Downloading {}.gz'.format(epw_filename))
                s3.download_fileobj(bucket, '{}/weather/{}.gz'.format(prefix, epw_filename), f_gz)
                with open(sim_dir / 'weather' / epw_filename, 'wb') as f_out:
                    logger.debug('Extracting {}'.format(epw_filename))
                    f_out.write(gzip.decompress(f_gz.getvalue()))

        for building_id, upgrade_idx in jobs_d['batch']:
            sim_id = 'bldg{:07d}up{:02d}'.format(building_id, 0 if upgrade_idx is None else upgrade_idx + 1)
            osw = cls.create_osw(cfg, sim_id, building_id, upgrade_idx)
            with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
                json.dump(osw, f, indent=4)
            with open(sim_dir / 'os_stdout.log', 'w') as f_out:
                try:
                    logger.debug('Running {}'.format(sim_id))
                    subprocess.run(
                        ['openstudio', 'run', '-w', 'in.osw'],
                        check=True,
                        stdout=f_out,
                        stderr=subprocess.STDOUT,
                        cwd=str(sim_dir)
                    )
                except subprocess.CalledProcessError:
                    pass

            cls.cleanup_sim_dir(sim_dir)

            logger.debug('Uploading simulation outputs')
            for dirpath, dirnames, filenames in os.walk(sim_dir):
                # Remove the asset directories from upload
                if pathlib.Path(dirpath) == sim_dir:
                    for dirname in asset_dirs:
                        dirnames.remove(dirname)
                for filename in filenames:
                    filepath = pathlib.Path(dirpath, filename)
                    logger.debug('Uploading {}'.format(filepath.relative_to(sim_dir)))
                    s3.upload_file(
                        str(filepath),
                        bucket,
                        str(pathlib.Path(prefix, 'results', sim_id, filepath.relative_to(sim_dir)))
                    )

            logger.debug('Writing output data to Firehose')
            datapoint_out_filepath = sim_dir / 'run' / 'data_point_out.json'
            out_osw_filepath = sim_dir / 'out.osw'
            if os.path.isfile(out_osw_filepath):
                out_osw = read_out_osw(out_osw_filepath)
                dp_out = flatten_datapoint_json(read_data_point_out_json(datapoint_out_filepath))
                if dp_out is None:
                    dp_out = {}
                dp_out.update(out_osw)
                dp_out['_id'] = sim_id
                for key in dp_out.keys():
                    dp_out[to_camelcase(key)] = dp_out.pop(key)
                # TODO: write dp_out to firehose

            logger.debug('Clearing out simulation directory')
            for item in set(os.listdir(sim_dir)).difference(asset_dirs):
                if os.path.isdir(item):
                    shutil.rmtree(item)
                elif os.path.isfile(item):
                    os.remove(item)


if __name__ == '__main__':
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
            }
        },
    })
    print(AwsBatch.LOGO)
    if 'AWS_BATCH_JOB_ARRAY_INDEX' in os.environ:
        job_id = int(os.environ['AWS_BATCH_JOB_ARRAY_INDEX'])
        s3_bucket = os.environ['S3_BUCKET']
        s3_prefix = os.environ['S3_PREFIX']
        AwsBatch.run_job(job_id, s3_bucket, s3_prefix)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('project_filename')
        args = parser.parse_args()
        batch = AwsBatch(args.project_filename)
        batch.push_image()
        batch.run_batch()
