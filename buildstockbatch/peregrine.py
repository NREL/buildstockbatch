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
import json
import logging as logging_
import math
import os
import re
import subprocess
from .hpc import HPCBatchBase

logger = logging_.getLogger(__name__)


class PeregrineBatch(HPCBatchBase):

    sys_image_dir = '/projects/enduse/openstudio_singularity_images' # TODO ???
    hpc_name = 'peregrine'
    min_sims_per_job = 48

    @property
    def output_dir(self):
        output_dir = self.cfg.get(
            'output_directory',
            os.path.join('/scratch/{}'.format(os.environ['USER']), os.path.basename(self.project_dir))
        )
        return output_dir

    def queue_jobs(self, array_ids=None):

        peregrine_cfg = self.cfg['peregrine']
        with open(os.path.join(self.output_dir, 'job001.json'), 'r') as f:
            job_json = json.load(f)
            n_sims_per_job = len(job_json['batch'])
            del job_json
        minutes_per_sim = peregrine_cfg.get('minutes_per_sim', 3)
        if not array_ids:
            jobjson_re = re.compile(r'job(\d+).json')
            array_ids = map(
                lambda m: int(m.group(1)),
                filter(lambda m: m is not None, map(jobjson_re.match, (os.listdir(self.output_dir))))
            )
        queue = peregrine_cfg.get('queue', 'batch-h')
        nodetype = peregrine_cfg.get('nodetype', 'haswell')
        allocation = peregrine_cfg['allocation']

        # TODO this is not used anymore - delete?
        def array_id_generator(array_spec):
            for array_group in array_spec.split(','):
                array_range = tuple(map(int, array_group.split('-')))
                if len(array_range) == 1:
                    yield array_range[0]
                else:
                    assert(len(array_range) == 2)
                    for x in range(array_range[0], array_range[1] + 1):
                        yield x

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
        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        job_ids = []
        for array_id in array_ids:
            args = [
                'qsub',
                '-v', 'PROJECTFILE,PBS_ARRAYID',
                '-q', queue,
                '-A', allocation,
                '-l', 'feature={}'.format(nodetype),
                '-l', 'walltime={}'.format(walltime),
                '-N', 'buildstock-{}'.format(array_id),
                '-o', os.path.join(self.output_dir, 'job.out-{}'.format(array_id)),
                peregrine_sh
            ]
            # TODO support -l qos=high
            env['PBS_ARRAYID'] = str(array_id)
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
            job_ids.append(jobid)
            logger.debug('Job id: {}'.format(jobid))
        with open(os.path.join(self.output_dir, 'job_ids.txt'), 'w') as f:
            for jobid in job_ids:
                f.write('{}\n'.format(jobid))
        return job_ids

    def queue_post_processing(self, after_jobids):

        allocation = self.cfg['peregrine']['allocation']

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
            '-W', 'depend=afterok:{}'.format(':'.join(after_jobids)),
            '-q', 'bigmem',
            '-A', allocation,
            '-l', 'feature=256GB',
            '-l', 'walltime=1:30:00',
            '-N', 'buildstock-post',
            '-o', os.path.join(self.output_dir, 'postprocessing.out'),
            peregrine_sh
        ]
        print('args are `{}` with env `{}`'.format(args, env))
        subprocess.run(args, env=env)

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

        # TODO This is broken right now - reevaluate
        jobids = self.queue_jobs(
            n_sims_per_job,
            peregrine_cfg.get('minutes_per_sim', 3),
            ','.join(map(str, jobs_to_restart)),
            peregrine_cfg.get('queue', 'batch-h'),
            peregrine_cfg.get('nodetype', 'haswell'),
            allocation
        )

        # TODO I think the allocation entry here is also broken...
        self.queue_post_processing(jobids, allocation)

    def get_dask_client(self):
        cl = LocalCluster(local_dir=os.path.join(self.output_dir, 'dask_worker_space'))
        return Client(cl)


def main():
    logging_.config.dictConfig({
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
        batch.run_batch()


if __name__ == '__main__':
    main()
