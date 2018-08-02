import os
import shutil
import logging
import argparse
import subprocess
import math
import functools
import itertools
import json
import uuid
import zipfile
import time
import shlex

import requests
from joblib import Parallel, delayed

from buildstockbatch.base import BuildStockBatchBase


class PeregrineBatch(BuildStockBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        output_dir = self.output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logging.debug('Output directory = {}'.format(output_dir))

        singularity_image_path = os.path.join(output_dir, 'openstudio.simg')
        if not os.path.isfile(singularity_image_path):
            logging.debug('Downloading singularity image')
            simg_url = \
                'https://s3.amazonaws.com/openstudio-builds/{ver}/OpenStudio-{ver}.{sha}-Singularity.simg'.format(
                    ver=self.OS_VERSION,
                    sha=self.OS_SHA
                )
            r = requests.get(simg_url, stream=True)
            with open(singularity_image_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            logging.debug('Downloaded singularity image to {}'.format(singularity_image_path))

    @property
    def output_dir(self):
        output_dir = self.cfg.get(
            'output_directory',
            os.path.join('/scratch/{}'.format(os.environ['USER']), os.path.basename(self.project_dir))
        )
        return output_dir

    @property
    def weather_dir(self):
        weather_dir = os.path.join(self.output_dir, 'weather')
        if not os.path.exists(weather_dir):
            os.makedirs(weather_dir)
            self._get_weather_files()
        return weather_dir

    @property
    def results_dir(self):
        results_dir = os.path.join(self.output_dir, 'results')
        assert(os.path.isdir(results_dir))
        return results_dir

    def run_sampling(self):
        logging.debug('Sampling')
        args = [
            'singularity',
            'exec',
            '--contain',
            '--home', self.buildstock_dir,
            'openstudio.simg',
            'ruby',
            'resources/run_sampling.rb',
            '-p', self.cfg['project_directory'],
            '-n', str(self.cfg['baseline']['n_datapoints']),
            '-o', 'buildstock.csv'
        ]
        subprocess.run(args, check=True, env=os.environ, cwd=self.output_dir)
        destination_dir = os.path.join(self.output_dir, 'housing_characteristics')
        if os.path.exists(destination_dir):
            shutil.rmtree(destination_dir)
        shutil.copytree(
            os.path.join(self.project_dir, 'housing_characteristics'),
            destination_dir
        )
        assert(os.path.isdir(destination_dir))
        shutil.move(
            os.path.join(self.buildstock_dir, 'resources', 'buildstock.csv'),
            destination_dir
        )
        return os.path.join(destination_dir, 'buildstock.csv')

    def run_batch(self, n_jobs=200, nodetype='haswell', queue='batch-h'):
        self.run_sampling()
        n_datapoints = self.cfg['baseline']['n_datapoints']
        n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)

        # This is the maximum number of jobs we'll submit for this batch
        n_sims_per_job = math.ceil(n_sims / n_jobs)
        # Have at least 48 simulations per job
        n_sims_per_job = max(n_sims_per_job, 48)

        nodes_per_nodetype = {
            '16core': 16,
            '64GB': 24,
            '256GB': 16,
            '24core': 24,
            'haswell': 24
        }

        # Estimated wall time
        walltime = math.ceil(n_sims_per_job / nodes_per_nodetype[nodetype]) * 9 * 60

        baseline_sims = zip(range(1, n_datapoints + 1), itertools.repeat(None))
        upgrade_sims = itertools.product(range(1, n_datapoints + 1), range(len(self.cfg.get('upgrades', []))))
        all_sims = itertools.chain(baseline_sims, upgrade_sims)

        for i in itertools.count(1):
            batch = list(itertools.islice(all_sims, n_sims_per_job))
            if not batch:
                break
            logging.info('Queueing job {} ({} simulations)'.format(i, len(batch)))
            job_json_filename = os.path.join(self.output_dir, 'job{:03d}.json'.format(i))
            with open(job_json_filename, 'w') as f:
                json.dump({
                    'job_num': i,
                    'batch': batch,
                }, f, indent=4)

        # Queue up simulations
        here = os.path.dirname(os.path.abspath(__file__))
        peregrine_sh = os.path.join(here, 'peregrine.sh')
        args = [
            'qsub',
            '-v', 'PROJECTFILE',
            '-q', queue,
            '-l', 'feature={}'.format(nodetype),
            '-l', 'walltime={}'.format(walltime),
            '-N', 'buildstocksims',
            '-t', '1-{}'.format(i - 1),
            '-o', os.path.join(self.output_dir, 'job.out'),
            peregrine_sh
        ]
        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            encoding='utf-8'
        )
        try:
            resp.check_returncode()
        except subprocess.CalledProcessError as ex:
            print(ex.stderr)
            raise
        jobid = resp.stdout.strip()
        logging.debug('Job ids: ' + jobid)

        # Queue up post processing
        env = {}
        env.update(os.environ)
        env.update({
            'POSTPROCESS': '1',
            'PROJECTFILE': self.project_filename
        })
        args = [
            'qsub',
            '-v', 'PROJECTFILE,POSTPROCESS',
            '-W', 'depend=afterokarray:{}'.format(jobid),
            '-q', 'short',
            '-l', 'walltime=20:00',
            '-N', 'buildstock_post',
            '-o', os.path.join(self.output_dir, 'postprocessing.out'),
            peregrine_sh
        ]
        subprocess.run(args, env=env)

    def run_job_batch(self, job_array_number):
        job_json_filename = os.path.join(self.output_dir, 'job{:03d}.json'.format(job_array_number))
        with open(job_json_filename, 'r') as f:
            args = json.load(f)

        run_building_d = functools.partial(
            delayed(self.run_building),
            self.project_dir,
            self.buildstock_dir,
            self.weather_dir,
            self.output_dir,
            self.cfg
        )
        tick = time.time()
        with Parallel(n_jobs=-1, verbose=9) as parallel:
            parallel(itertools.starmap(run_building_d, args['batch']))
        tick = time.time() - tick
        logging.info('Simulation time: {:.2f} minutes'.format(tick / 60.))

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, output_dir, cfg, i, upgrade_idx=None):
        sim_id = str(uuid.uuid4())

        # Create the simulation directory
        sim_dir = os.path.join(output_dir, 'results', sim_id)
        os.makedirs(sim_dir)

        # Generate the osw for this simulation
        osw = cls.create_osw(sim_id, cfg, i, upgrade_idx)
        with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
            json.dump(osw, f, indent=4)

        # Copy other necessary stuff into the simulation directory
        dirs_to_mount = [
            os.path.join(buildstock_dir, 'measures'),
            os.path.join(project_dir, 'seeds'),
            weather_dir,
        ]

        # Call singularity to run the simulation
        args = [
            'singularity', 'exec',
            '--contain',
            '--pwd', '/var/simdata/openstudio',
            '-B', '{}:/var/simdata/openstudio'.format(sim_dir),
            '-B', '{}:/lib/resources'.format(os.path.join(buildstock_dir, 'resources')),
            '-B', '{}:/lib/housing_characteristics'.format(os.path.join(output_dir, 'housing_characteristics'))
        ]
        runscript = [
            'ln -s /lib /var/simdata/openstudio/lib'
        ]
        for src in dirs_to_mount:
            container_mount = '/' + os.path.basename(src)
            args.extend(['-B', '{}:{}:ro'.format(src, container_mount)])
            container_symlink = os.path.join('/var/simdata/openstudio', os.path.basename(src))
            runscript.append('ln -s {} {}'.format(*map(shlex.quote, (container_mount, container_symlink))))
        runscript.extend([
            'openstudio run -w in.osw --debug'
        ])
        args.extend([
            'openstudio.simg',
            'bash', '-x'
        ])
        logging.debug(' '.join(args))
        with open(os.path.join(sim_dir, 'singularity_output.log'), 'w') as f_out:
            subprocess.run(
                args,
                check=True,
                input='\n'.join(runscript).encode('utf-8'),
                stdout=f_out,
                stderr=subprocess.STDOUT,
                cwd=output_dir
            )

        # Clean up the symbolic links we created in the container
        for dir in dirs_to_mount:
            os.unlink(os.path.join(sim_dir, os.path.basename(dir)))
        os.unlink(os.path.join(sim_dir, 'lib'))

        # Remove files already in data_point.zip
        zipfilename = os.path.join(sim_dir, 'run', 'data_point.zip')
        if os.path.isfile(zipfilename):
            with zipfile.ZipFile(zipfilename, 'r') as zf:
                for filename in zf.namelist():
                    filepath = os.path.join(sim_dir, 'run', filename)
                    if os.path.exists(filepath):
                        os.remove(filepath)


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(levelname)s:%(asctime)s:%(message)s'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    args = parser.parse_args()
    batch = PeregrineBatch(args.project_filename)
    job_array_number = int(os.environ.get('PBS_ARRAYID', 0))
    postprocess = os.environ.get('POSTPROCESS', '0').lower() in ('true', 't', '1', 'y', 'yes')
    if job_array_number:
        batch.run_job_batch(job_array_number)
    elif postprocess:
        batch.process_results()
    else:
        batch.run_batch()


if __name__ == '__main__':
    main()
