import argparse
import os
import itertools
import functools
import glob
import json
import re
import shutil
import subprocess
import time
import logging
import uuid

from joblib import Parallel, delayed
from pandas.io.json import json_normalize

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

    def run_sampling(self):
        logging.debug('Sampling')
        args = [
            'docker',
            'run',
            '--rm',
            '-v', '{}:/var/simdata/openstudio'.format(self.buildstock_dir),
            'nrel/openstudio:{}'.format(self.OS_VERSION),
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(self.cfg['baseline']['n_datapoints']),
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
    def run_building(cls, project_dir, buildstock_dir, weather_dir, cfg, i, upgrade_idx=None):
        sim_id = str(uuid.uuid4())
        sim_dir = os.path.join(project_dir, 'localResults', sim_id)

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

    def run_batch(self, n_jobs=-1):
        self.run_sampling()
        n_datapoints = self.cfg['baseline']['n_datapoints']
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir,
            self.cfg
        )
        baseline_sims = map(run_building_d, range(1, n_datapoints + 1))
        upgrade_sims = []
        for i in range(len(self.cfg.get('upgrades', []))):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), range(1, n_datapoints + 1)))
        all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

    @staticmethod
    def _read_data_point_out_json(filename):
        with open(filename, 'r') as f:
            d = json.load(f)
        d['_id'] = os.path.basename(os.path.dirname(os.path.dirname(os.path.abspath(filename))))
        return d

    @staticmethod
    def to_camelcase(x):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', x)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    def process_results(self):
        results_dir = os.path.join(self.project_dir, 'localResults')
        assert(os.path.isdir(results_dir))
        datapoint_output_jsons = glob.glob(os.path.join(results_dir, '*', 'run', 'data_point_out.json'))
        df = json_normalize(Parallel(n_jobs=-1)(map(delayed(self._read_data_point_out_json), datapoint_output_jsons)))
        df.rename(columns=self.to_camelcase, inplace=True)
        df.set_index('_id', inplace=True)
        cols_to_keep = [
            'build_existing_model.building_id',
            'apply_upgrade.upgrade_name',
            'apply_upgrade.applicable'
        ]
        cols_to_keep.extend(filter(lambda x: x.startswith('building_characteristics_report.'), df.columns))
        cols_to_keep.extend(filter(lambda x: x.startswith('simulation_output_report.'), df.columns))
        df = df[cols_to_keep]
        df.to_csv(os.path.join(results_dir, 'results.csv'), index=False)
        df.to_pickle(os.path.join(results_dir, 'results.pkl'))


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