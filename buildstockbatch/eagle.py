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
from glob import glob
import json
import os
import logging
import math
import pathlib
import re
import shutil
import subprocess
import yamale

from .hpc import HPCBatchBase, SimulationExists

logger = logging.getLogger(__name__)


class EagleBatch(HPCBatchBase):

    sys_image_dir = '/home/nmerket/buildstock/assets'  # TODO ??? A bad idea for portability - sys_image_dir much?
    hpc_name = 'eagle'
    min_sims_per_job = 36 * 2  # TODO This interface needs to be exposed via the yaml

    local_scratch = pathlib.Path('/tmp/scratch')
    local_project_dir = local_scratch / 'project'
    local_buildstock_dir = local_scratch / 'buildstock'
    local_weather_dir = local_scratch / 'weather'
    local_output_dir = local_scratch
    local_singularity_img = local_scratch / 'openstudio.simg'

    @staticmethod
    def validate_project(project_file):
        schema = yamale.make_schema(os.path.join(os.path.dirname(__file__), 'schemas', 'eagle.yaml'))
        data = yamale.make_data(project_file)
        _ = yamale.validate(schema, data)

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
            sim_id, remote_sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(output_dir, 'results'))
        except SimulationExists:
            return

        # Clear the local directory if it exists for some reason
        local_sim_dir = cls.local_output_dir / 'results' / sim_id
        shutil.rmtree(local_sim_dir, ignore_errors=True)
        local_sim_dir.mkdir(parents=True, exist_ok=True)

        # Run the building using the local copies of the resources
        HPCBatchBase.run_building(
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
        cls.modify_fs_metadata(local_sim_dir, cfg)
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
        account = eagle_cfg.get('account')

        # Estimate the wall time in minutes
        cores_per_node = 36
        walltime = math.ceil(math.ceil(n_sims_per_job / cores_per_node) * minutes_per_sim)

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
            '--output=job.out-%a',
            '--job-name=bstk',
            eagle_sh
        ]
        if account:
            args.insert(1, '--account={}'.format(account))
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
            '--dependency=afterany:{}'.format(':'.join(after_jobids)),
            '--mem=184000',
            '--job-name=bstkpost',
            '--output=postprocessing.out',
            eagle_sh
        ]
        if account:
            args.insert(1, '--account={}'.format(account))

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
        cl = LocalCluster(
            n_workers=18,
            threads_per_worker=2,
            local_dir='/tmp/scratch/dask_worker_space'
        )
        return Client(cl)

    @staticmethod
    def modify_fs_metadata(sim_dir, cfg):
        to_modify = [y for x in os.walk(sim_dir) for y in glob(os.path.join(x[0], '*.*'))]
        try:
            _ = [os.chmod(file, 0o664) for file in to_modify]
            _ = [shutil.chown(file, group=cfg['eagle']['account']) for file in to_modify]
        except:
            logger.warning('Unable to chmod/chown results of simulation in directory {}'.format(sim_dir))


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
