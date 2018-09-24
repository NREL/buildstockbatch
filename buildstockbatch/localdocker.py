import argparse
import os
import itertools
import functools
import json
import shutil
import time
import logging

from joblib import Parallel, delayed
import docker

from buildstockbatch.base import BuildStockBatchBase

logger = logging.getLogger(__name__)


class LocalDockerBatch(BuildStockBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        self.docker_client = docker.DockerClient.from_env()

        logger.debug('Pulling docker image')
        self.docker_client.images.pull(self.docker_image())

    @classmethod
    def docker_image(cls):
        return 'nrel/openstudio:{}'.format(cls.OS_VERSION)

    def run_sampling(self, n_datapoints=None):
        if n_datapoints is None:
            n_datapoints = self.cfg['baseline']['n_datapoints']
        logger.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        tick = time.time()
        container_output = self.docker_client.containers.run(
            self.docker_image(),
            [
                'ruby',
                'resources/run_sampling.rb',
                '-p', self.cfg['project_directory'],
                '-n', str(n_datapoints),
                '-o', 'buildstock.csv'
            ],
            remove=True,
            volumes={
                self.buildstock_dir: {'bind': '/var/simdata/openstudio', 'mode': 'rw'}
            },
            name='buildstock_sampling'
        )
        tick = time.time() - tick
        for line in container_output.decode('utf-8').split('\n'):
            logger.debug(line)
        logger.debug('Sampling took {:.1f} seconds'.format(tick))
        destination_filename = os.path.join(self.project_dir, 'housing_characteristics', 'buildstock.csv')
        if os.path.exists(destination_filename):
            os.remove(destination_filename)
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_filename
        )
        return destination_filename

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, results_dir, cfg, i, upgrade_idx=None):
        sim_id = 'bldg{:07d}up{:02d}'.format(i, 0 if upgrade_idx is None else upgrade_idx + 1)
        sim_dir = os.path.join(results_dir, sim_id)

        bind_mounts = [
            (sim_dir, '/var/simdata/openstudio', 'rw'),
            (os.path.join(buildstock_dir, 'measures'), '/var/simdata/openstudio/measures', 'ro'),
            (os.path.join(buildstock_dir, 'resources'), '/var/simdata/openstudio/lib/resources', 'ro'),
            (os.path.join(project_dir, 'housing_characteristics'), '/var/simdata/openstudio/lib/housing_characteristics', 'ro'),
            (os.path.join(project_dir, 'seeds'), '/var/simdata/openstudio/seeds', 'ro'),
            (weather_dir, '/var/simdata/openstudio/weather', 'ro')
        ]
        docker_volume_mounts = dict([(key, {'bind': bind, 'mode': mode}) for key, bind, mode in bind_mounts])

        osw = cls.create_osw(sim_id, cfg, i, upgrade_idx)

        os.makedirs(sim_dir)
        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        docker_client = docker.client.from_env()
        container_output = docker_client.containers.run(
            cls.docker_image(),
            [
                'openstudio',
                'run',
                '-w', 'in.osw'
            ],
            remove=True,
            volumes=docker_volume_mounts,
            name=sim_id
        )
        with open(os.path.join(sim_dir, 'docker_output.log'), 'wb') as f_out:
            f_out.write(container_output)

        # Clean up directories created with the docker mounts
        for dirname in ('lib', 'measures', 'seeds', 'weather'):
            shutil.rmtree(os.path.join(sim_dir, dirname), ignore_errors=True)

        cls.cleanup_sim_dir(sim_dir)

    def run_batch(self, n_jobs=-1):
        if 'downselect' in self.cfg:
            self.downselect()
        else:
            self.run_sampling()
        n_datapoints = self.cfg['baseline']['n_datapoints']
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir,
            self.results_dir,
            self.cfg
        )
        baseline_sims = map(run_building_d, range(1, n_datapoints + 1))
        upgrade_sims = []
        for i in range(len(self.cfg.get('upgrades', []))):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), range(1, n_datapoints + 1)))
        all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

    @property
    def results_dir(self):
        results_dir = self.cfg.get(
            'output_directory',
            os.path.join(self.project_dir, 'localResults')
        )
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
    parser.add_argument('project_filename')
    parser.add_argument('-j', type=int,
                        help='Number of parallel simulations, -1 is all cores, -2 is all cores except one',
                        default=-1)
    parser.add_argument('--skipsims',
                        help='Skip simulating buildings, useful for when the simulations are already done',
                        action='store_true')
    args = parser.parse_args()
    batch = LocalDockerBatch(args.project_filename)
    if not args.skipsims:
        batch.run_batch(n_jobs=args.j)
    batch.process_results()


if __name__ == '__main__':
    main()
