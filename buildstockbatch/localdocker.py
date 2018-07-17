import argparse
from collections import OrderedDict
import datetime as dt
import os
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
        weather_dir = os.path.join(self.project_dir, 'weather')
        os.makedirs(weather_dir, exist_ok=True)
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
                logging.debug('Extracting weather files to: {}'.format(weather_dir))
                zf.extractall(weather_dir)
        else:
            logging.debug('Downloading weather files')
            r = requests.get(self.cfg['weather_files_url'], stream=True)
            with tempfile.TemporaryFile() as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                f.seek(0)
                with zipfile.ZipFile(f, 'r') as zf:
                    logging.debug('Extracting weather files to: {}'.format(weather_dir))
                    zf.extractall(weather_dir)

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
    def run_building(cls, samples_csv, project_dir, buildstock_dir, cfg, i):
        sim_id = str(uuid.uuid4())

        bind_mounts = OrderedDict([
            (project_dir, '/var/simdata/openstudio/project'),
            (os.path.join(buildstock_dir, 'measures'), '/var/simdata/openstudio/project/measures'),
            (os.path.join(buildstock_dir, 'resources'), '/var/simdata/openstudio/project/lib/resources'),
            (os.path.join(project_dir, 'housing_characteristics'), '/var/simdata/openstudio/project/lib/housing_characteristics')
        ])

        osw = {
            'id': sim_id,
            'steps': [
                {
                    'measure_dir_name': 'BuildExistingModel',
                    'arguments': {
                        'building_id': i,
                        'workflow_json': 'measure-info.json',
                        'weight': cfg['baseline']['n_buildings_represented'] / cfg['baseline']['n_datapoints']
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
            'root': bind_mounts[project_dir],
            'run_directory': 'localResults/{}'.format(sim_id),
            'file_paths': [

            ],
            'measure_paths': [
                'measures'
            ],
            'seed_file': 'seeds/EmptySeedModel.osm',
            'weather_file': 'weather/Placeholder.epw'
        }

        sim_dir = os.path.join(project_dir, 'localResults', sim_id)
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
            '-w', 'project/localResults/{}/in.osw'.format(sim_id),
            '--debug'
        ])
        logging.debug(' '.join(args))
        subprocess.run(args, check=True)

    def run_batch(self, n_jobs=-1):
        samples_csv = self.run_sampling()
        n_datapoints = self.cfg['baseline']['n_datapoints']
        Parallel(n_jobs=n_jobs, verbose=10)(
            delayed(self.run_building)(
                samples_csv,
                self.project_dir,
                self.buildstock_dir,
                self.cfg,
                i
            )
            for i in range(1, n_datapoints + 1)
        )

def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    parser.add_argument('-j', type=int,
                        help='Number of parallel simulations, -1 is all cores, -2 is all cores except one')
    args = parser.parse_args()
    batch = LocalDockerBatch(args.project_filename)
    batch.run_batch(n_jobs=args.j)


if __name__ == '__main__':
    main()