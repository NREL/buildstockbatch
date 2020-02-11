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
import docker
import functools
import itertools
from joblib import Parallel, delayed
import json
import logging
import os
import pandas as pd
import shutil
import sys
import tempfile

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch.sampler import ResidentialDockerSampler, CommercialSobolDockerSampler, PrecomputedDockerSampler

logger = logging.getLogger(__name__)


class DockerBatchBase(BuildStockBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        self.docker_client = docker.DockerClient.from_env()
        # On Windows, check that the docker server is responding (that you have the Docker Daemon running)
        # this will hopefully prevent other people from trying to debug bogus issues.
        if os.name == 'nt':
            try:
                self.docker_client.ping()
            except:  # noqa: E722 (allow bare except in this case because error is weird non-class Windows API error)
                logger.error('The docker server did not respond, make sure Docker Desktop is started then retry.')
                raise RuntimeError('The docker server did not respond, make sure Docker Desktop is started then retry.')

        if self.stock_type == 'residential':
            self.sampler = ResidentialDockerSampler(
                self.docker_image,
                self.cfg,
                self.buildstock_dir,
                self.project_dir
            )
        elif self.stock_type == 'commercial':
            sampling_algorithm = self.cfg['baseline'].get('sampling_algorithm', 'sobol')
            if sampling_algorithm == 'sobol':
                self.sampler = CommercialSobolDockerSampler(
                    self.project_dir,
                    self.cfg,
                    self.buildstock_dir,
                    self.project_dir
                )
            elif sampling_algorithm == 'precomputed':
                logger.info('calling precomputed sampler')
                self.sampler = PrecomputedDockerSampler(
                    self.output_dir,
                    self.cfg,
                    self.buildstock_dir,
                    self.project_dir
                )
            else:
                raise NotImplementedError('Sampling algorithem "{}" is not implemented.'.format(sampling_algorithm))
        else:
            raise KeyError('stock_type = "{}" is not valid'.format(self.stock_type))

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
        logger.debug(f'Pulling docker image: {self.docker_image()}')
        self.docker_client.images.pull(self.docker_image())

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
                     cfg, i, upgrade_idx=None):
        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(results_dir, 'simulation_output'))
        except SimulationExists:
            return

        bind_mounts = [
            (sim_dir, '', 'rw'),
            (os.path.join(buildstock_dir, 'measures'), 'measures', 'ro'),
            (os.path.join(buildstock_dir, 'resources'), 'lib/resources', 'ro'),
            (os.path.join(project_dir, 'housing_characteristics'), 'lib/housing_characteristics', 'ro'),
            (os.path.join(project_dir, 'seeds'), 'seeds', 'ro'),
            (weather_dir, 'weather', 'ro')
        ]
        docker_volume_mounts = dict([(key, {'bind': f'/var/simdata/openstudio/{bind}', 'mode': mode}) for key, bind, mode in bind_mounts])  # noqa E501
        for bind in bind_mounts:
            dir_to_make = os.path.join(sim_dir, *bind[1].split('/'))
            if not os.path.exists(dir_to_make):
                os.makedirs(dir_to_make)

        osw = cls.create_osw(cfg, sim_id, building_id=i, upgrade_idx=upgrade_idx)

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
        for dirname in ('lib', 'measures', 'seeds', 'weather'):
            shutil.rmtree(os.path.join(sim_dir, dirname), ignore_errors=True)

        cls.cleanup_sim_dir(sim_dir)

    def run_batch(self, n_jobs=-1, measures_only=False, sampling_only=False):
        if 'downselect' in self.cfg:
            buildstock_csv_filename = self.downselect()
        else:
            buildstock_csv_filename = self.run_sampling()

        if sampling_only:
            return

        df = pd.read_csv(buildstock_csv_filename, index_col=0)
        building_ids = df.index.tolist()
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir,
            self.docker_image,
            self.results_dir,
            measures_only,
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
        Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

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
        help='Number of parallel simulations, -1 is all cores, -2 is all cores except one',
        default=-1
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
