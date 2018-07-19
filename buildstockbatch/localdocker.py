import argparse
from collections import OrderedDict
import datetime as dt
import os
import itertools
import functools
import json
import shutil
import subprocess
import tempfile
import time
import logging
import uuid
import zipfile

from joblib import Parallel, delayed
import requests

from buildstockbatch.base import BuildStockBatchBase


class LocalDockerBatch(BuildStockBatchBase):

    OS_VERSION = '2.6.0'

    def __init__(self, project_filename):
        super().__init__(project_filename)

        logging.debug('Pulling docker image')
        subprocess.run(
            ['docker', 'pull', 'nrel/openstudio:{}'.format(self.OS_VERSION)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT
        )

        # Get the weather files
        self.weather_dir = tempfile.TemporaryDirectory(dir=self.project_dir, prefix='weather')
        local_weather_dir = os.path.join(self.project_dir, 'weather')
        for filename in os.listdir(local_weather_dir):
            shutil.copy(os.path.join(local_weather_dir, filename), self.weather_dir.name)
        if 'weather_files_path' in self.cfg:
            logging.debug('Copying weather files')
            if os.path.isabs(self.cfg['weather_files_path']):
                weather_file_path = os.path.abspath(self.cfg['weather_files_path'])
            else:
                weather_file_path = os.path.abspath(
                    os.path.join(
                        os.path.dirname(self.project_filename),
                        self.cfg['weather_files_path']
                    )
                )
            with zipfile.ZipFile(weather_file_path, 'r') as zf:
                logging.debug('Extracting weather files to: {}'.format(self.weather_dir.name))
                zf.extractall(self.weather_dir.name)
        else:
            logging.debug('Downloading weather files')
            r = requests.get(self.cfg['weather_files_url'], stream=True)
            with tempfile.TemporaryFile() as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.seek(0)
                with zipfile.ZipFile(f, 'r') as zf:
                    logging.debug('Extracting weather files to: {}'.format(self.weather_dir.name))
                    zf.extractall(self.weather_dir.name)

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

        bind_mounts = OrderedDict([
            (sim_dir, '/var/simdata/openstudio'),
            (os.path.join(buildstock_dir, 'measures'), '/var/simdata/openstudio/measures'),
            (os.path.join(buildstock_dir, 'resources'), '/var/simdata/openstudio/lib/resources'),
            (os.path.join(project_dir, 'housing_characteristics'), '/var/simdata/openstudio/lib/housing_characteristics'),
            (os.path.join(project_dir, 'seeds'), '/var/simdata/openstudio/seeds'),
            (weather_dir, '/var/simdata/openstudio/weather')
        ])

        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': {
                        'building_id': i,
                        'workflow_json': 'measure-info.json',
                        'sample_weight': cfg['baseline']['n_buildings_represented'] / cfg['baseline']['n_datapoints']
                    }
                },
                {
                    'measure_dir_name': 'BuildingCharacteristicsReport',
                    'arguments': {}
                },
                {
                    'measure_dir_name': 'SimulationOutputReport',
                    'arguments': {}
                },
                {
                    'measure_dir_name': 'ServerDirectoryCleanup',
                    'arguments': {}
                }
            ],
            'created_at': dt.datetime.now().isoformat(),
            'measure_paths': [
                'measures'
            ],
            'seed_file': 'seeds/EmptySeedModel.osm',
            'weather_file': 'weather/Placeholder.epw'
        }

        if upgrade_idx is not None:
            measure_d = cfg['upgrades'][upgrade_idx]
            expanded_measure_d = {
                'measure_dir_name': 'ApplyUpgrade',
                'arguments': {
                    'upgrade_name': measure_d['upgrade_name'],
                    'run_measure': 1
                }
            }
            for opt_num, option in enumerate(measure_d['options'], 1):
                expanded_measure_d['arguments']['option_{}'.format(opt_num)] = option['option']
                for arg in ('apply_logic', 'lifetime'):
                    if arg not in option:
                        continue
                    expanded_measure_d['arguments']['option_{}_{}'.format(opt_num, arg)] = option[arg]
                for cost_num, cost in enumerate(option['costs'], 1):
                    for arg in ('value', 'multiplier'):
                        if arg not in cost:
                            continue
                        expanded_measure_d['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                            cost[arg]
            if 'package_apply_logic' in measure_d:
                expanded_measure_d['package_apply_logic'] = measure_d['package_apply_logic']

            osw['steps'].insert(1, expanded_measure_d)

        os.makedirs(sim_dir)
        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        args = [
            'docker',
            'run',
            '--rm'
        ]
        for k, v in bind_mounts.items():
            args.extend(['-v', '{}:{}'.format(k, v)])
        args.extend([
            'nrel/openstudio:{}'.format(cls.OS_VERSION),
            'openstudio',
            'run',
            '-w', 'in.osw',
            '--debug'
        ])
        logging.debug(' '.join(args))
        with open(os.path.join(sim_dir, 'docker_output.log'), 'w') as f_out:
            subprocess.run(args, check=True, stdout=f_out, stderr=subprocess.STDOUT)

    def run_batch(self, n_jobs=-1):
        self.run_sampling()
        n_datapoints = self.cfg['baseline']['n_datapoints']
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir.name,
            self.cfg
        )
        baseline_sims = map(run_building_d, range(1, n_datapoints + 1))
        upgrade_sims = []
        for i in range(len(self.cfg.get('upgrades', []))):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), range(1, n_datapoints + 1)))
        all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    parser.add_argument('-j', type=int,
                        help='Number of parallel simulations, -1 is all cores, -2 is all cores except one',
                        default=-1)
    args = parser.parse_args()
    batch = LocalDockerBatch(args.project_filename)
    batch.run_batch(n_jobs=args.j)


if __name__ == '__main__':
    main()