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

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)
        # LocalDocker specific code goes here

    @property
    def docker_image(self):
        return 'nrel/openstudio:{}'.format(self.os_version)


class LocalDockerBatch(DockerBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        logger.debug(f'Pulling docker image: {self.docker_image}')
        self.docker_client.images.pull(self.docker_image)

        # Create simulation_output dir
        sim_out_ts_dir = os.path.join(self.results_dir, 'simulation_output', 'timeseries')
        os.makedirs(sim_out_ts_dir, exist_ok=True)
        for i in range(0, len(self.cfg.get('upgrades', [])) + 1):
            os.makedirs(os.path.join(sim_out_ts_dir, f'up{i:02d}'), exist_ok=True)

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
    def run_building(cls, project_dir, buildstock_dir, weather_dir, docker_image, results_dir, measures_only,
                     n_datapoints, cfg, i, upgrade_idx=None):

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
        if os.path.exists(os.path.join(buildstock_dir, 'resources', 'hpxml-measures')):
            bind_mounts += [(os.path.join(buildstock_dir, 'resources', 'hpxml-measures'),
                            'resources/hpxml-measures', 'ro')]
        docker_volume_mounts = dict([(key, {'bind': f'/var/simdata/openstudio/{bind}', 'mode': mode}) for key, bind, mode in bind_mounts])  # noqa E501
        for bind in bind_mounts:
            dir_to_make = os.path.join(sim_dir, *bind[1].split('/'))
            if not os.path.exists(dir_to_make):
                os.makedirs(dir_to_make)

        osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)

        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        docker_client = docker.client.from_env()
        args = [
            'openstudio',
            'run',
            '-w', 'in.osw',
        ]
        if measures_only:
            args.insert(2, '--measures_only')
        extra_kws = {}
        if sys.platform.startswith('linux'):
            extra_kws['user'] = f'{os.getuid()}:{os.getgid()}'
        container_output = docker_client.containers.run(
            docker_image,
            args,
            remove=True,
            volumes=docker_volume_mounts,
            name=sim_id,
            **extra_kws
        )
        with open(os.path.join(sim_dir, 'docker_output.log'), 'wb') as f_out:
            f_out.write(container_output)

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
