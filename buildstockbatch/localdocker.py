import argparse
import os
import itertools
import functools
import json
import shutil
import subprocess
import time
import logging

from joblib import Parallel, delayed

from buildstockbatch.base import BuildStockBatchBase


class LocalDockerBatch(BuildStockBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)

        logging.debug('Pulling docker image')
        subprocess.run(
            ['docker', 'pull', 'nrel/openstudio:{}'.format(self.OS_VERSION)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )

    def run_sampling(self, n_datapoints=None):
        if n_datapoints is None:
            n_datapoints = self.cfg['baseline']['n_datapoints']
        logging.debug('Sampling, n_datapoints={}'.format(n_datapoints))
        args = [
            'docker',
            'run',
            '--rm',
            '-v', '{}:/var/simdata/openstudio'.format(self.buildstock_dir),
            'nrel/openstudio:{}'.format(self.OS_VERSION),
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(n_datapoints),
            '-o', 'buildstock.csv'
        ]
        tick = time.time()
        subprocess.run(args, check=True)
        tick = time.time() - tick
        logging.debug('Sampling took {:.1f} seconds'.format(tick))
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
            (sim_dir, '/var/simdata/openstudio'),
            (os.path.join(buildstock_dir, 'measures'), '/var/simdata/openstudio/measures', 'ro'),
            (os.path.join(buildstock_dir, 'resources'), '/var/simdata/openstudio/lib/resources', 'ro'),
            (os.path.join(project_dir, 'housing_characteristics'), '/var/simdata/openstudio/lib/housing_characteristics', 'ro'),
            (os.path.join(project_dir, 'seeds'), '/var/simdata/openstudio/seeds', 'ro'),
            (weather_dir, '/var/simdata/openstudio/weather', 'ro')
        ]

        osw = cls.create_osw(sim_id, cfg, i, upgrade_idx)

        os.makedirs(sim_dir)
        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        args = [
            'docker',
            'run',
            '--rm'
        ]
        for x in bind_mounts:
            args.extend(['-v', ':'.join(x)])
        args.extend([
            'nrel/openstudio:{}'.format(cls.OS_VERSION),
            'openstudio',
            'run',
            '-w', 'in.osw'
        ])
        logging.debug(' '.join(args))
        with open(os.path.join(sim_dir, 'docker_output.log'), 'w') as f_out:
            subprocess.run(args, check=True, stdout=f_out, stderr=subprocess.STDOUT)

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
    logging.basicConfig(
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(levelname)s:%(asctime)s:%(message)s'
    )
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