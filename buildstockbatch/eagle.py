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
import re
import subprocess

from .hpc import HPCBatchBase

logger = logging.getLogger(__name__)


class EagleBatch(HPCBatchBase):

    sys_image_dir = ''
    hpc_name = 'eagle'
    min_sims_per_job = 36 * 2

    @property
    def output_dir(self):
        output_dir = self.cfg.get(
            'output_directory',
            os.path.join('/scratch/{}'.format(os.environ['USER']), os.path.basename(self.project_dir))
        )
        return output_dir

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
        account = eagle_cfg.get('account')

        # Estimate the wall time in minutes
        cores_per_node = 36
        walltime = math.ceil(n_sims_per_job / cores_per_node) * minutes_per_sim

        # Queue up simulations
        here = os.path.dirname(os.path.abspath(__file__))
        eagle_sh = os.path.join(here, 'eagle.sh')
        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        args = [
            'sbatch',
            '--time={}'.format(walltime),
            '--export=PROJECTFILE',
            '--array={}'.format(array_spec),
            '--output="{}"'.format(os.path.join(self.output_dir, 'job.out-%a')),
            '--job-name=buildstock',
            eagle_sh
        ]
        if account:
            args.insert(1, '--account={}'.format(account))
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
            logger.error(ex.stderr)
            raise
        m = re.search(r'Submitted batch job (\d+)', resp.stdout)
        if not m:
            logger.error('Did not receive job id back from sbatch:')
            for line in resp.stdout.split('\n'):
                logger.error('    ' + line)
            raise RuntimeError('Didn\'t receive job id back from sbatch')
        job_id = int(m.group(1))
        return [job_id]

    def queue_post_processing(self, after_jobids):

        # Configuration values
        account = self.cfg['eagle'].get('account')
        walltime = self.cfg['eagle'].get('postprocessing', {}).get('time', '1:30:00')

        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        env['POSTPROCESS'] = '1'

        here = os.path.dirname(os.path.abspath(__file__))
        eagle_sh = os.path.join(here, 'eagle.sh')

        args = [
            'sbatch',
            '--time={}'.format(walltime),
            '--export=PROJECTFILE,POSTPROCESS',
            '--dependency=afterok:{}'.format(':'.join(after_jobids)),
            '--mem=184000',
            '--job-name=buildstock-post',
            '--output="{}"'.format(os.path.join(self.output_dir, 'postprocessing.out')),
            eagle_sh
        ]
        if account:
            args.insert(1, '--account={}'.format(account))

        resp = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            encoding='utf-8'
        )
        for line in resp.stdout.split('\n'):
            logger.debug('sbatch: {}'.format(line))

    def get_dask_client(self):
        cl = LocalCluster(local_dir='/tmp/scratch/dask_worker_space')
        return Client(cl)


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
    print(HPCBatchBase.LOGO)
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
