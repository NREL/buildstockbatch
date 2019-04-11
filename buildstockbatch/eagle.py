# -*- coding: utf-8 -*-

"""
buildstockbatch.eagle
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with Eagle

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client, LocalCluster
import json
import os
import logging
import math
import pathlib
import re
import shutil
import subprocess
import yaml

from .hpc import HPCBatchBase, SimulationExists

logger = logging.getLogger(__name__)


class EagleBatch(HPCBatchBase):

    sys_image_dir = '/shared-projects/buildstock/singularity_images'
    hpc_name = 'eagle'
    min_sims_per_job = 36 * 2

    local_scratch = pathlib.Path('/tmp/scratch')
    local_project_dir = local_scratch / 'project'
    local_buildstock_dir = local_scratch / 'buildstock'
    local_weather_dir = local_scratch / 'weather'
    local_output_dir = local_scratch
    local_singularity_img = local_scratch / 'openstudio.simg'

    @property
    def output_dir(self):
        output_dir = self.cfg.get(
            'output_directory',
            os.path.join('/scratch/{}'.format(os.environ['USER']), os.path.basename(self.project_dir))
        )
        return output_dir

    @staticmethod
    def clear_and_copy_dir(src, dst):
        if os.path.exists(dst):
            shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)

    def run_job_batch(self, job_array_number):

        # Move resources to the local scratch directory
        self.clear_and_copy_dir(
            pathlib.Path(self.project_dir) / 'seeds',
            self.local_project_dir / 'seeds'
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / 'resources',
            self.local_buildstock_dir / 'resources'
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / 'measures',
            self.local_buildstock_dir / 'measures'
        )
        self.clear_and_copy_dir(
            self.weather_dir,
            self.local_weather_dir
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.output_dir) / 'housing_characteristics',
            self.local_output_dir / 'housing_characteristics'
        )
        if os.path.exists(self.local_singularity_img):
            os.remove(self.local_singularity_img)
        shutil.copy2(self.singularity_image, self.local_singularity_img)

        # Run the job batch as normal
        super(EagleBatch, self).run_job_batch(job_array_number)

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, output_dir, singularity_image, cfg, i,
                     upgrade_idx=None):

        # Check for an existing results directory
        try:
            sim_id, remote_sim_dir = cls.make_sim_dir(
                i,
                upgrade_idx,
                os.path.join(output_dir, 'results', 'simulation_output')
            )
        except SimulationExists:
            return

        # Run the building using the local copies of the resources
        local_sim_dir = HPCBatchBase.run_building(
            str(cls.local_project_dir),
            str(cls.local_buildstock_dir),
            str(cls.local_weather_dir),
            str(cls.local_output_dir),
            str(cls.local_singularity_img),
            cfg,
            i,
            upgrade_idx
        )

        # Copy the results to the remote directory
        cls.clear_and_copy_dir(local_sim_dir, remote_sim_dir)
        shutil.rmtree(local_sim_dir, ignore_errors=True)

    def queue_jobs(self, array_ids=None):
        eagle_cfg = self.cfg['eagle']
        minutes_per_sim = eagle_cfg.get('minutes_per_sim', 3)
        with open(os.path.join(self.output_dir, 'job001.json'), 'r') as f:
            job_json = json.load(f)
            n_sims_per_job = len(job_json['batch'])
            del job_json
        if array_ids:
            array_spec = ','.join(map(str, array_ids))
        else:
            jobjson_re = re.compile(r'job(\d+).json')
            array_max = max(map(
                lambda m: int(m.group(1)),
                filter(lambda m: m is not None, map(jobjson_re.match, (os.listdir(self.output_dir))))
            ))
            array_spec = '1-{}'.format(array_max)
        account = eagle_cfg['account']

        # Estimate the wall time in minutes
        cores_per_node = 36
        walltime = math.ceil(math.ceil(n_sims_per_job / cores_per_node) * minutes_per_sim)

        # Queue up simulations
        here = os.path.dirname(os.path.abspath(__file__))
        eagle_sh = os.path.join(here, 'eagle.sh')
        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        env['MY_CONDA_ENV'] = os.environ['CONDA_PREFIX']
        args = [
            'sbatch',
            '--account={}'.format(account),
            '--time={}'.format(walltime),
            '--export=PROJECTFILE,MY_CONDA_ENV',
            '--array={}'.format(array_spec),
            '--output=job.out-%a',
            '--job-name=bstk',
            eagle_sh
        ]
        logger.debug(' '.join(args))
        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            encoding='utf-8',
            cwd=self.output_dir
        )
        try:
            resp.check_returncode()
        except subprocess.CalledProcessError as ex:
            logger.error(ex.stderr)
            raise
        for line in resp.stdout.split('\n'):
            logger.debug('sbatch:' + line)
        m = re.search(r'Submitted batch job (\d+)', resp.stdout)
        if not m:
            logger.error('Did not receive job id back from sbatch:')
            raise RuntimeError('Didn\'t receive job id back from sbatch')
        job_id = m.group(1)
        return [job_id]

    def queue_post_processing(self, after_jobids):

        # Configuration values
        account = self.cfg['eagle']['account']
        walltime = self.cfg['eagle'].get('postprocessing', {}).get('time', '1:30:00')

        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        env['POSTPROCESS'] = '1'
        env['MY_CONDA_ENV'] = os.environ['CONDA_PREFIX']
        env['OUT_DIR'] = self.output_dir

        here = os.path.dirname(os.path.abspath(__file__))
        eagle_post_sh = os.path.join(here, 'eagle_postprocessing.sh')

        args = [
            'sbatch',
            '--account={}'.format(account),
            '--time={}'.format(walltime),
            '--export=PROJECTFILE,POSTPROCESS,MY_CONDA_ENV,OUT_DIR',
            '--dependency=afterany:{}'.format(':'.join(after_jobids)),
            '--job-name=bstkpost',
            '--output=postprocessing.out',
            '--nodes=1',
            ':',
            '--mem=180000',
            '--output=dask_workers.out',
            '--nodes={}'.format(self.cfg['eagle'].get('postprocessing', {}).get('n_workers', 2)),
            eagle_post_sh
        ]

        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            encoding='utf-8',
            cwd=self.output_dir
        )
        for line in resp.stdout.split('\n'):
            logger.debug('sbatch: {}'.format(line))

    def get_dask_client(self):
        return Client(scheduler_file=os.path.join(self.output_dir, 'dask_scheduler.json'))


logging_config = {
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
    }


def user_cli():
    logging.config.dictConfig(logging_config)
    print(HPCBatchBase.LOGO)
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    args = parser.parse_args()
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError(
            'The project file {} doesn\'t exist'.format(args.project_filename)
        )
    project_filename = os.path.abspath(args.project_filename)
    with open(project_filename, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.SafeLoader)
    eagle_sh = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'eagle.sh')
    assert(os.path.exists(eagle_sh))
    out_dir = cfg['output_directory']
    if os.path.exists(out_dir):
        raise FileExistsError(
            'The output directory {} already exists. Please delete it or choose another.'.format(out_dir)
        )
    logger.info('Creating output directory {}'.format(out_dir))
    os.makedirs(out_dir)
    env = {}
    env.update(os.environ)
    env['PROJECTFILE'] = project_filename
    env['MY_CONDA_ENV'] = os.environ['CONDA_PREFIX']
    args = [
        'sbatch',
        '--time={}'.format(cfg['eagle'].get('sampling', {}).get('time', 60)),
        '--account={}'.format(cfg['eagle']['account']),
        '--nodes=1',
        '--export=PROJECTFILE,MY_CONDA_ENV',
        '--output=sampling.out',
        eagle_sh
    ]
    logger.info('Submitting sampling job to task scheduler')
    subprocess.run(args, env=env, cwd=out_dir, check=True)
    logger.info('Run squeue -u $USER to monitor the progress of your jobs')


def main():
    logging.config.dictConfig(logging_config)
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    args = parser.parse_args()
    batch = EagleBatch(args.project_filename)
    job_array_number = int(os.environ.get('SLURM_ARRAY_TASK_ID', 0))
    post_process = os.environ.get('POSTPROCESS', '0').lower() in ('true', 't', '1', 'y', 'yes')
    if job_array_number:
        batch.run_job_batch(job_array_number)
    elif post_process:
        batch.process_results()
    else:
        batch.run_batch()


if __name__ == '__main__':
    main()
