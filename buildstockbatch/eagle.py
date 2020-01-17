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
from dask.distributed import Client
import json
import os
import logging
import math
import pathlib
import re
import shutil
import subprocess
import sys
import yaml

from .hpc import HPCBatchBase, SimulationExists, get_bool_env_var

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

    @staticmethod
    def validate_project(project_file):
        super(EagleBatch, EagleBatch).validate_project(project_file)
        # Eagle specific validation goes here

    @property
    def output_dir(self):
        output_dir = self.cfg.get(
            'output_directory',
            os.path.join('/scratch/{}'.format(os.environ.get('USER', 'user')), os.path.basename(self.project_dir))
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
            '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY',
            '--array={}'.format(array_spec),
            '--output=job.out-%a',
            '--job-name=bstk',
            eagle_sh
        ]
        if os.environ.get('SLURM_JOB_QOS'):
            args.insert(-1, '--qos={}'.format(os.environ.get('SLURM_JOB_QOS')))

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

    def queue_post_processing(self, after_jobids=[], upload_only=False, hipri=False):

        # Configuration values
        account = self.cfg['eagle']['account']
        walltime = self.cfg['eagle'].get('postprocessing', {}).get('time', '1:30:00')

        # Clear out some files that cause problems if we're rerunning this.

        if not upload_only:
            for subdir in ('parquet', 'results_csvs'):
                subdirpath = pathlib.Path(self.output_dir, 'results', subdir)
                if subdirpath.exists():
                    shutil.rmtree(subdirpath)

        for filename in ('dask_scheduler.json', 'dask_scheduler.out', 'dask_workers.out', 'postprocessing.out'):
            filepath = pathlib.Path(self.output_dir, filename)
            if filepath.exists():
                os.remove(filepath)

        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        env['MY_CONDA_ENV'] = os.environ['CONDA_PREFIX']
        env['OUT_DIR'] = self.output_dir
        env['UPLOADONLY'] = str(upload_only)
        here = os.path.dirname(os.path.abspath(__file__))
        eagle_post_sh = os.path.join(here, 'eagle_postprocessing.sh')

        args = [
            'sbatch',
            '--account={}'.format(account),
            '--time={}'.format(walltime),
            '--export=PROJECTFILE,MY_CONDA_ENV,OUT_DIR,UPLOADONLY',
            '--job-name=bstkpost',
            '--output=postprocessing.out',
            '--nodes=1',
            ':',
            '--mem=180000',
            '--output=dask_workers.out',
            '--nodes={}'.format(self.cfg['eagle'].get('postprocessing', {}).get('n_workers', 2)),
            eagle_post_sh
        ]

        if after_jobids:
            args.insert(4, '--dependency=afterany:{}'.format(':'.join(after_jobids)))

        if os.environ.get('SLURM_JOB_QOS'):
            args.insert(-1, '--qos={}'.format(os.environ.get('SLURM_JOB_QOS')))
        elif hipri:
            args.insert(-1, '--qos=high')

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


def user_cli(argv=sys.argv[1:]):
    '''
    This is the user entry point for running buildstockbatch on Eagle
    '''

    # set up logging, currently based on within-this-file hard-coded config
    logging.config.dictConfig(logging_config)

    # print BuildStockBatch logo
    print(HPCBatchBase.LOGO)

    # CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    parser.add_argument(
        '--hipri',
        action='store_true',
        help='Submit this job to the high priority queue. Uses 2x node hours.'
    )
    parser.add_argument(
        '-m', '--measures_only',
        action='store_true',
        help='Only apply the measures, but don\'t run simulations. Useful for debugging.'
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        '--postprocessonly',
        help='Only do postprocessing, useful for when the simulations are already done',
        action='store_true'
    )
    group.add_argument(
        '--uploadonly',
        help='Only upload to S3, useful when postprocessing is already done. Ignores the upload flag in yaml',
        action='store_true'
    )
    group.add_argument(
        '--validateonly',
        help='Only validate the project YAML file and references. Nothing is executed',
        action='store_true'
    )
    group.add_argument(
        '--samplingonly',
        help='Run the sampling only.',
        action='store_true'
    )

    # parse CLI arguments
    args = parser.parse_args(argv)

    # load the yaml project file
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError(
            'The project file {} doesn\'t exist'.format(args.project_filename)
        )
    project_filename = os.path.abspath(args.project_filename)
    with open(project_filename, 'r') as f:
        cfg = yaml.load(f, Loader=yaml.SafeLoader)

    # validate the project, and in case of the --validateonly flag return True if validation passes
    EagleBatch.validate_project(project_filename)
    if args.validateonly:
        return True

    # if the project has already been run, simply queue the correct post-processing step
    if args.postprocessonly or args.uploadonly:
        eagle_batch = EagleBatch(project_filename)
        eagle_batch.queue_post_processing(upload_only=args.uploadonly, hipri=args.hipri)
        return True

    # otherwise, queue up the whole eagle buildstockbatch process
    # the main work of the first Eagle job is to run the sampling script ...
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
    env['MEASURESONLY'] = str(int(args.measures_only))
    env['SAMPLINGONLY'] = str(int(args.samplingonly))
    subargs = [
        'sbatch',
        '--time={}'.format(cfg['eagle'].get('sampling', {}).get('time', 60)),
        '--account={}'.format(cfg['eagle']['account']),
        '--nodes=1',
        '--export=PROJECTFILE,MY_CONDA_ENV,MEASURESONLY,SAMPLINGONLY',
        '--output=sampling.out',
        eagle_sh
    ]
    if args.hipri:
        subargs.insert(-1, '--qos=high')
    logger.info('Submitting sampling job to task scheduler')
    subprocess.run(subargs, env=env, cwd=out_dir, check=True)
    logger.info('Run squeue -u $USER to monitor the progress of your jobs')
    # eagle.sh calls main()


def main():
    """
    Determines which piece of work is to be run right now, on this process, on
    this node. There are four types of work that may need to be done:

    - initialization, sampling, and queuing other work (job_array_number == 0)
    - run a batch of simulations (job_array_number > 0)
    - post-process results (job_array_number == 0 and POSTPROCESS)
    - upload results to Athena (job_array_number == 0 and POSTPROCESS and UPLOADONLY)

    The context for the work is deinfed by the project_filename (project .yml file),
    which is used to initialize an EagleBatch object.
    """

    # set up logging, currently based on within-this-file hard-coded config
    logging.config.dictConfig(logging_config)

    # only direct script argument is the project .yml file
    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')
    args = parser.parse_args()

    # initialize the EagleBatch object
    batch = EagleBatch(args.project_filename)
    # other arguments/cues about which part of the process we are in are
    # encoded in slurm job environment variables
    job_array_number = int(os.environ.get('SLURM_ARRAY_TASK_ID', 0))
    post_process = get_bool_env_var('POSTPROCESS')
    upload_only = get_bool_env_var('UPLOADONLY')
    measures_only = get_bool_env_var('MEASURESONLY')
    sampling_only = get_bool_env_var('SAMPLINGONLY')
    if job_array_number:
        # if job array number is non-zero, run the batch job
        # Simulation should not be scheduled for sampling only
        assert(not sampling_only)
        batch.run_job_batch(job_array_number)
    elif post_process:
        # else, we might be in a post-processing step
        # Postprocessing should not have been scheduled if measures only or sampling only are run
        assert(not measures_only)
        assert(not sampling_only)
        if upload_only:
            batch.process_results(skip_combine=True, force_upload=True)
        else:
            batch.process_results()
    else:
        # default job_array_number == 0 task is to kick the whole BuildStock
        # process off, that is, to create samples and then create batch jobs
        # to run them
        batch.run_batch(sampling_only)


if __name__ == '__main__':
    main()
