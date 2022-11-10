# -*- coding: utf-8 -*-

"""
buildstockbatch.localdocker
~~~~~~~~~~~~~~~
This object contains the code required for execution of local docker batch simulations

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client, LocalCluster
import docker
import functools
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import os
import pandas as pd
import re
import shutil
import sys
import tarfile
import tempfile

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch import postprocessing
from .utils import log_error_details, ContainerRuntime
from docker.errors import ImageNotFound
from buildstockbatch.__version__ import __version__ as bsb_version

logger = logging.getLogger(__name__)


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

        self._weather_dir = None

        # FIXME: something better and more general than os_hescore_dir
        if self.os_hescore_dir:
            logger.debug("Building HEScore docker image")
            self.docker_client.images.build(
                path=self.os_hescore_dir,
                tag=self.docker_image,
                dockerfile="resstock/Dockerfile",
                rm=True,
                buildargs={'os_version': self.os_version}
            )
            logger.debug("Docker image built")
        else:
            try:
                self.docker_client.images.get(self.docker_image)
            except ImageNotFound:
                self.docker_client.images.pull(self.docker_image)

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)
        # LocalDocker specific code goes here

    @property
    def docker_image(self):
        if self.os_hescore_dir:
            return 'nrel/hescore-hpxml-openstudio'
        else:
            return 'nrel/openstudio:{}'.format(self.os_version)


class LocalDockerBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        logger.debug(f'Pulling docker image: {self.docker_image}')

        # Create simulation_output dir
        sim_out_ts_dir = os.path.join(self.results_dir, 'simulation_output', 'timeseries')
        os.makedirs(sim_out_ts_dir, exist_ok=True)
        for i in range(0, len(self.cfg.get('upgrades', [])) + 1):
            os.makedirs(os.path.join(sim_out_ts_dir, f'up{i:02d}'), exist_ok=True)

        # Install custom gems to a volume that will be used by all workers
        if self.cfg.get('baseline', dict()).get('custom_gems', False):
            logger.info('Installing custom gems to docker volume: buildstockbatch_custom_gems')

            docker_client = docker.client.from_env()

            # Create a volume to store the custom gems
            docker_client.volumes.create(name='buildstockbatch_custom_gems', driver='local')
            simdata_vol = docker_client.volumes.create(name='buildstockbatch_simdata_temp', driver='local')

            # Define directories to be mounted in the container
            mnt_gem_dir = '/var/oscli/gems'
            # Install custom gems to be used in the docker container
            local_gemfile_path = os.path.join(self.buildstock_dir, 'resources', 'Gemfile')
            mnt_gemfile_path_orig = "/var/oscli/gemfile/Gemfile"
            docker_volume_mounts = {
                'buildstockbatch_custom_gems': {'bind': mnt_gem_dir, 'mode': 'rw'},
                local_gemfile_path: {'bind': mnt_gemfile_path_orig, 'mode': 'ro'},
                simdata_vol.name: {'bind': '/var/simdata/openstudio', 'mode': 'rw'},
            }

            # Check that the Gemfile exists
            if not os.path.exists(local_gemfile_path):
                print(f'local_gemfile_path = {local_gemfile_path}')
                raise AttributeError('baseline:custom_gems = True, but did not find Gemfile in /resources directory')

            # Make the buildstock/resources/.custom_gems dir to store logs
            local_log_dir = os.path.join(self.buildstock_dir, 'resources', '.custom_gems')
            if not os.path.exists(local_log_dir):
                os.makedirs(local_log_dir)

            # Run bundler to install the custom gems
            mnt_gemfile_path = f"{mnt_gem_dir}/Gemfile"
            bundle_install_cmd = f'/bin/bash -c "cp {mnt_gemfile_path_orig} {mnt_gemfile_path} && bundle install --path={mnt_gem_dir} --gemfile={mnt_gemfile_path}"'  # noqa: E501
            logger.debug(f'Running {bundle_install_cmd}')
            container_output = docker_client.containers.run(
                self.docker_image,
                bundle_install_cmd,
                remove=True,
                volumes=docker_volume_mounts,
                name='install_custom_gems'
            )
            with open(os.path.join(local_log_dir, 'bundle_install_output.log'), 'wb') as f_out:
                f_out.write(container_output)

            # Report out custom gems loaded by OpenStudio CLI
            check_active_gems_cmd = f'openstudio --bundle {mnt_gemfile_path} --bundle_path {mnt_gem_dir} ' \
                                    '--bundle_without native_ext gem_list'
            container_output = docker_client.containers.run(
                self.docker_image,
                check_active_gems_cmd,
                remove=True,
                volumes=docker_volume_mounts,
                name='list_custom_gems'
            )
            gem_list_log = os.path.join(local_log_dir, 'openstudio_gem_list_output.log')
            with open(gem_list_log, 'wb') as f_out:
                f_out.write(container_output)
            simdata_vol.remove()
            logger.debug(f'Review custom gems list at: {gem_list_log}')

    @staticmethod
    def validate_project(project_file):
        super(LocalDockerBatch, LocalDockerBatch).validate_project(project_file)
        # LocalDocker specific code goes here

    @property
    def weather_dir(self):
        if self._weather_dir is None:
            self._weather_dir = tempfile.TemporaryDirectory(dir=self.results_dir, prefix='weather')
            self._get_weather_files()
        return self._weather_dir.name

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, os_hescore_dir, docker_image, results_dir,
                     measures_only, n_datapoints, cfg, i, upgrade_idx=None):

        upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(results_dir, 'simulation_output'))
        except SimulationExists:
            return
        bind_mounts = [
            (sim_dir, '', 'rw'),
            (os.path.join(buildstock_dir, 'measures'), 'measures', 'ro'),
            (os.path.join(buildstock_dir, 'resources'), 'lib/resources', 'ro'),
            (os.path.join(project_dir, 'housing_characteristics'), 'lib/housing_characteristics', 'ro'),
            (weather_dir, 'weather', 'ro')
        ]

        # ResStock-hpxml measure directory
        if os.path.exists(os.path.join(buildstock_dir, 'resources', 'hpxml-measures')):
            bind_mounts.append(
                (os.path.join(buildstock_dir, 'resources', 'hpxml-measures'), 'resources/hpxml-measures', 'ro')
            )

        # OS-HEScore weather directory
        if os_hescore_dir and os.path.exists(os_hescore_dir):
            bind_mounts.append((os.path.join(os_hescore_dir, 'weather'), '/opt/OpenStudio-HEScore/weather', 'ro'))

        docker_volume_mounts = {
            key: {
                'bind': bind if bind.startswith('/') else f'/var/simdata/openstudio/{bind}',
                'mode': mode
            }
            for key, bind, mode in bind_mounts
        }

        for bind in bind_mounts:
            if bind[1].startswith('/'):
                continue
            dir_to_make = os.path.join(sim_dir, *bind[1].split('/'))
            if not os.path.exists(dir_to_make):
                os.makedirs(dir_to_make)

        if cfg.get('baseline', dict()).get('custom_gems', False):
            mnt_custom_gem_dir = '/var/oscli/gems'
            docker_volume_mounts['buildstockbatch_custom_gems'] = {'bind': mnt_custom_gem_dir, 'mode': 'ro'}

        osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)

        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        docker_client = docker.client.from_env()
        run_cmd = [
            'openstudio',
            'run',
            '-w', 'in.osw',
        ]
        if cfg.get('baseline', dict()).get('custom_gems', False):
            run_cmd = [
                'openstudio',
                '--bundle', f'{mnt_custom_gem_dir}/Gemfile',
                '--bundle_path', f'{mnt_custom_gem_dir}',
                '--bundle_without', 'native_ext',
                'run', '-w', 'in.osw',
                '--debug'
            ]
        if measures_only:
            if cfg.get('baseline', dict()).get('custom_gems', False):
                run_cmd.insert(8, '--measures_only')
            else:
                run_cmd.insert(2, '--measures_only')

        env_vars = {}
        env_vars['BUILDSTOCKBATCH_VERSION'] = bsb_version

        extra_kws = {}
        if sys.platform.startswith('linux'):
            extra_kws['user'] = f'{os.getuid()}:{os.getgid()}'

        container = docker_client.containers.run(
            docker_image,
            run_cmd,
            remove=True,
            detach=True,
            volumes=docker_volume_mounts,
            name=sim_id,
            environment=env_vars,
            **extra_kws
        )
        with open(os.path.join(sim_dir, 'docker_output.log'), 'wb') as f_out:
            for x in container.logs(stream=True):
                f_out.write(x)

        # Clean up directories created with the docker mounts
        for dirname in ('lib', 'measures', 'weather'):
            shutil.rmtree(os.path.join(sim_dir, dirname), ignore_errors=True)

        fs = LocalFileSystem()
        cls.cleanup_sim_dir(
            sim_dir,
            fs,
            f"{results_dir}/simulation_output/timeseries",
            upgrade_id,
            i
        )

        # Read data_point_out.json
        reporting_measures = cls.get_reporting_measures(cfg)
        dpout = postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, i)
        return dpout

    def run_batch(self, n_jobs=None, measures_only=False, sampling_only=False):
        buildstock_csv_filename = self.sampler.run_sampling()

        if sampling_only:
            return

        df = pd.read_csv(buildstock_csv_filename, index_col=0)
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir,
            self.os_hescore_dir,
            self.docker_image,
            self.results_dir,
            measures_only,
            n_datapoints,
            self.cfg
        )
        upgrade_sims = []
        for i in range(len(self.cfg.get('upgrades', []))):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), building_ids))
        if not self.skip_baseline_sims:
            baseline_sims = map(run_building_d, building_ids)
            all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        else:
            all_sims = itertools.chain(*upgrade_sims)
        if n_jobs is None:
            client = docker.client.from_env()
            n_jobs = client.info()['NCPU']
        dpouts = Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

        sim_out_dir = os.path.join(self.results_dir, 'simulation_output')

        results_job_json_filename = os.path.join(sim_out_dir, 'results_job0.json.gz')
        with gzip.open(results_job_json_filename, 'wt', encoding='utf-8') as f:
            json.dump(dpouts, f)
        del dpouts

        # FIXME temporarily comment out for testing
        sim_out_tarfile_name = os.path.join(sim_out_dir, 'simulations_job0.tar.gz')
        logger.debug(f'Compressing simulation outputs to {sim_out_tarfile_name}')
        with tarfile.open(sim_out_tarfile_name, 'w:gz') as tarf:
            for dirname in os.listdir(sim_out_dir):
                if re.match(r'up\d+', dirname) and os.path.isdir(os.path.join(sim_out_dir, dirname)):
                    tarf.add(os.path.join(sim_out_dir, dirname), arcname=dirname)
                    shutil.rmtree(os.path.join(sim_out_dir, dirname))

    @property
    def output_dir(self):
        return self.results_dir

    @property
    def results_dir(self):
        results_dir = self.cfg.get(
            'output_directory',
            os.path.join(self.project_dir, 'localResults')
        )
        results_dir = self.path_rel_to_projectfile(results_dir)
        if not os.path.isdir(results_dir):
            os.makedirs(results_dir)
        return results_dir

    def get_dask_client(self):
        cluster = LocalCluster(local_directory=os.path.join(self.results_dir, 'dask-tmp'))
        return Client(cluster)


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
            }
        },
    })
    parser = argparse.ArgumentParser()
    print(BuildStockBatchBase.LOGO)
    parser.add_argument('project_filename')
    parser.add_argument(
        '-j',
        type=int,
        help='Number of parallel simulations. Default: all cores available to docker.',
        default=None
    )
    parser.add_argument(
        '-m', '--measures_only',
        action='store_true',
        help='Only apply the measures, but don\'t run simulations. Useful for debugging.'
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--postprocessonly',
                       help='Only do postprocessing, useful for when the simulations are already done',
                       action='store_true')
    group.add_argument('--uploadonly',
                       help='Only upload to S3, useful when postprocessing is already done. Ignores the '
                       'upload flag in yaml', action='store_true')
    group.add_argument('--validateonly', help='Only validate the project YAML file and references. Nothing is executed',
                       action='store_true')
    group.add_argument('--samplingonly', help='Run the sampling only.',
                       action='store_true')
    args = parser.parse_args()
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError(f'The project file {args.project_filename} doesn\'t exist')

    # Validate the project, and in case of the --validateonly flag return True if validation passes
    LocalDockerBatch.validate_project(args.project_filename)
    if args.validateonly:
        return True
    batch = LocalDockerBatch(args.project_filename)
    if not (args.postprocessonly or args.uploadonly or args.validateonly):
        batch.run_batch(n_jobs=args.j, measures_only=args.measures_only, sampling_only=args.samplingonly)
    if args.measures_only or args.samplingonly:
        return
    if args.uploadonly:
        batch.process_results(skip_combine=True, force_upload=True)
    else:
        batch.process_results()


if __name__ == '__main__':
    main()
