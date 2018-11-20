# -*- coding: utf-8 -*-

"""
buildstockbatch.peregrine
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with peregrine

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client, LocalCluster
import functools
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pandas as pd
import random
import re
import requests
import shutil
import shlex
import subprocess
import time

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.sampler import ResidentialSingularitySampler, CommercialSobolSingularitySampler, \
    CommercialPrecomputedSingularitySampler


class PeregrineBatch(BuildStockBatchBase):

    def __init__(self, project_filename):
        super().__init__(project_filename)
        output_dir = self.output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logging.debug('Output directory = {}'.format(output_dir))

        if self.stock_type == 'residential':
            self.sampler = ResidentialSingularitySampler(
                self.singularity_image,
                self.output_dir,
                self.cfg,
                self.buildstock_dir,
                self.project_dir
            )
        elif self.stock_type == 'commercial':
            sampling_algorithm = self.cfg['baseline'].get('sampling_algorithm', 'sobol')
            if sampling_algorithm == 'sobol':
                self.sampler = CommercialSobolSingularitySampler(
                    self.output_dir,
                    self.cfg,
                    self.buildstock_dir,
                    self.project_dir
                )
            elif sampling_algorithm == 'precomputed':
                print('calling precomputed commercial sampler')
                self.sampler = CommercialPrecomputedSingularitySampler(
                    self.output_dir,
                    self.cfg,
                    self.buildstock_dir,
                    self.project_dir
                )
            else:
                raise NotImplementedError('Sampling algorithm "{}" is not implemented.'.format(sampling_algorithm))
        else:
            raise KeyError('stock_type = "{}" is not valid'.format(self.stock_type))

    @property
    def singularity_image(self):
        sys_image_dir = '/projects/res_stock/openstudio_singularity_images'
        if 'sys_image_dir' in self.cfg.keys():
            sys_image_dir = self.cfg['sys_image_dir']
        sys_image = os.path.join(sys_image_dir, 'OpenStudio-{ver}.{sha}-Singularity.simg'.format(
            ver=self.OS_VERSION,
            sha=self.OS_SHA
        ))
        logging.debug('Singularity image to use is `{}`'.format(sys_image))
        if os.path.isfile(sys_image):
            return sys_image
        else:
            singularity_image_path = os.path.join(self.output_dir, 'openstudio.simg')
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
            return singularity_image_path

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

    def _queue_jobs(self, n_sims_per_job, minutes_per_sim, array_spec, queue, nodetype, allocation):

        nodes_per_nodetype = {
            '16core': 16,
            '64GB': 24,
            '256GB': 16,
            '24core': 24,
            'haswell': 24
        }

        # Estimate wall time
        walltime = math.ceil(n_sims_per_job / nodes_per_nodetype[nodetype]) * minutes_per_sim * 60

        # Queue up simulations
        here = os.path.dirname(os.path.abspath(__file__))
        peregrine_sh = os.path.join(here, 'peregrine.sh')
        args = [
            'qsub',
            '-v', 'PROJECTFILE',
            '-q', queue,
            '-A', allocation,
            '-l', 'feature={}'.format(nodetype),
            '-l', 'walltime={}'.format(walltime),
            '-N', 'buildstock',
            '-t', array_spec,
            '-o', os.path.join(self.output_dir, 'job.out'),
            peregrine_sh
        ]
        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        print('args are `{}` with env `{}`'.format(args, env))
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
        logging.debug('Job id: ' + jobid)
        return jobid

    def _queue_post_processing(self, after_jobid, allocation):
        # Queue up post processing
        here = os.path.dirname(os.path.abspath(__file__))
        peregrine_sh = os.path.join(here, 'peregrine.sh')
        env = {}
        env.update(os.environ)
        env.update({
            'POSTPROCESS': '1',
            'PROJECTFILE': self.project_filename
        })
        args = [
            'qsub',
            '-v', 'PROJECTFILE,POSTPROCESS',
            '-W', 'depend=afterokarray:{}'.format(after_jobid),
            '-q', 'bigmem',
            '-A', allocation,
            '-l', 'feature=256GB',
            '-l', 'walltime=1:30:00',
            '-N', 'buildstock_post',
            '-o', os.path.join(self.output_dir, 'postprocessing.out'),
            peregrine_sh
        ]
        print('args are `{}` with env `{}`'.format(args, env))
        subprocess.run(args, env=env)

    def run_batch(self, n_jobs=200, nodetype='haswell', queue='batch-h', allocation='res_stock', minutes_per_sim=3):
        if 'downselect' in self.cfg:
            buildstock_csv_filename = self.downselect()
        else:
            buildstock_csv_filename = self.run_sampling()
        df = pd.read_csv(buildstock_csv_filename, index_col=0)
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)

        # This is the maximum number of jobs we'll submit for this batch
        n_sims_per_job = math.ceil(n_sims / n_jobs)
        # Have at least 48 simulations per job
        n_sims_per_job = max(n_sims_per_job, 48)

        baseline_sims = zip(building_ids, itertools.repeat(None))
        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get('upgrades', []))))
        all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        for i in itertools.count(1):
            batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
            if not batch:
                break
            logging.info('Queueing job {} ({} simulations)'.format(i, len(batch)))
            job_json_filename = os.path.join(self.output_dir, 'job{:03d}.json'.format(i))
            with open(job_json_filename, 'w') as f:
                json.dump({
                    'job_num': i,
                    'batch': batch,
                }, f, indent=4)

        jobid = self._queue_jobs(n_sims_per_job, minutes_per_sim, '1-{}'.format(i - 1), queue, nodetype, allocation)

        self._queue_post_processing(jobid, allocation)

    def pick_up_where_left_off(self):
        jobs_to_restart = []
        n_sims_per_job = 0
        for filename in os.listdir(self.output_dir):
            m_jobout = re.match(r'job.out-(\d+)$', filename)
            if m_jobout:
                array_id = int(m_jobout.group(1))
                logfile_path = os.path.join(self.output_dir, filename)
                with open(logfile_path, 'r') as f:
                    logfile_contents = f.read()
                if re.search(r'PBS: job killed: walltime \d+ exceeded limit \d+', logfile_contents):
                    jobs_to_restart.append(array_id)
                    with open(logfile_path + '.bak', 'a') as f:
                        f.write('\n')
                        f.write(logfile_contents)
                    os.remove(logfile_path)
                continue
            m_jobjson = re.match(r'job(\d+).json', filename)
            if m_jobjson:
                with open(os.path.join(self.output_dir, filename)) as f:
                    job_d = json.load(f)
                n_sims_per_job = max(len(job_d['batch']), n_sims_per_job)
        jobs_to_restart.sort()

        peregrine_cfg = self.cfg.get('peregrine', {})
        allocation = peregrine_cfg.get('allocation', 'res_stock')

        jobid = self._queue_jobs(
            n_sims_per_job,
            peregrine_cfg.get('minutes_per_sim', 3),
            ','.join(map(str, jobs_to_restart)),
            peregrine_cfg.get('queue', 'batch-h'),
            peregrine_cfg.get('nodetype', 'haswell'),
            allocation
        )

        self._queue_post_processing(jobid, allocation)

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
            self.singularity_image,
            self.cfg
        )
        tick = time.time()
        with Parallel(n_jobs=-1, verbose=9) as parallel:
            parallel(itertools.starmap(run_building_d, args['batch']))
        tick = time.time() - tick
        logging.info('Simulation time: {:.2f} minutes'.format(tick / 60.))

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, output_dir, singularity_image, cfg, i,
                     upgrade_idx=None):
        sim_id = 'bldg{:07d}up{:02d}'.format(i, 0 if upgrade_idx is None else upgrade_idx + 1)

        # Check to see if the simulation is done already and skip it if so.
        sim_dir = os.path.join(output_dir, 'results', sim_id)
        if os.path.exists(sim_dir):
            if os.path.exists(os.path.join(sim_dir, 'run', 'finished.job')):
                return
            elif os.path.exists(os.path.join(sim_dir, 'run', 'failed.job')):
                return
            else:
                shutil.rmtree(sim_dir)

        # Create the simulation directory
        os.makedirs(sim_dir)

        # Generate the osw for this simulation
        osw = cls.create_osw(cfg, sim_id, building_id=i, upgrade_idx=upgrade_idx)
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
            '-B', '{}:/lib/resources-shared'.format(os.path.join(buildstock_dir, 'resources', 'shared')),
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
            'openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems run -w in.osw --debug'
        ])
        args.extend([
            singularity_image,
            'bash', '-x'
        ])
        env = {}
        env.update(os.environ)
        env.pop('LANG', None)
        logging.debug(' '.join(args))
        with open(os.path.join(sim_dir, 'singularity_output.log'), 'w') as f_out:
            try:
                subprocess.run(
                    args,
                    check=True,
                    input='\n'.join(runscript).encode('utf-8'),
                    stdout=f_out,
                    stderr=subprocess.STDOUT,
                    cwd=output_dir,
                    env=env
                )
            except subprocess.CalledProcessError:
                pass
            finally:
                # Clean up the symbolic links we created in the container
                for mount_dir in dirs_to_mount + [os.path.join(sim_dir, 'lib')]:
                    try:
                        os.unlink(os.path.join(sim_dir, os.path.basename(mount_dir)))
                    except FileNotFoundError:
                        pass

                cls.cleanup_sim_dir(sim_dir, cfg['peregrine'].get('allocation', 'eedr'))

    def get_dask_client(self):
        cl = LocalCluster(local_dir=os.path.join(self.output_dir, 'dask_worker_space'))
        return Client(cl)


def main():
    logging.basicConfig(
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S',
        format='%(levelname)s:%(asctime)s:%(message)s'
    )
    parser = argparse.ArgumentParser()
    print(BuildStockBatchBase.LOGO)
    parser.add_argument('project_filename')
    args = parser.parse_args()
    batch = PeregrineBatch(args.project_filename)
    job_array_number = int(os.environ.get('PBS_ARRAYID', 0))
    post_process = os.environ.get('POSTPROCESS', '0').lower() in ('true', 't', '1', 'y', 'yes')
    pick_up = os.environ.get('PICKUP', '0').lower() in ('true', 't', '1', 'y', 'yes')
    if job_array_number:
        batch.run_job_batch(job_array_number)
    elif post_process:
        batch.process_results()
    elif pick_up:
        batch.pick_up_where_left_off()
    else:
        batch.run_batch(**batch.cfg.get('peregrine', {}))


if __name__ == '__main__':
    main()
