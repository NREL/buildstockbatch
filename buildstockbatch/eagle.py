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
import datetime as dt
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import delayed, Parallel
import json
import logging
import math
import os
import pandas as pd
import pathlib
import random
import re
import requests
import shlex
import shutil
import subprocess
import sys
import time
import yaml
import csv

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch.utils import log_error_details, get_error_details, ContainerRuntime
from buildstockbatch import postprocessing

logger = logging.getLogger(__name__)


def get_bool_env_var(varname):
    return os.environ.get(varname, '0').lower() in ('true', 't', '1', 'y', 'yes')


class EagleBatch(BuildStockBatchBase):

    CONTAINER_RUNTIME = ContainerRuntime.SINGULARITY

    sys_image_dir = '/shared-projects/buildstock/singularity_images'
    hpc_name = 'eagle'
    min_sims_per_job = 36 * 2

    local_scratch = pathlib.Path('/tmp/scratch')
    local_project_dir = local_scratch / 'project'
    local_buildstock_dir = local_scratch / 'buildstock'
    local_weather_dir = local_scratch / 'weather'
    local_output_dir = local_scratch / 'output'
    local_singularity_img = local_scratch / 'openstudio.simg'
    local_housing_characteristics_dir = local_scratch / 'housing_characteristics'

    def __init__(self, project_filename):
        super().__init__(project_filename)
        output_dir = pathlib.Path(self.output_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logger.debug('Output directory = {}'.format(output_dir))
        weather_dir = self.weather_dir  # noqa E841

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

    @property
    def results_dir(self):
        results_dir = os.path.join(self.output_dir, 'results')
        assert(os.path.isdir(results_dir))
        return results_dir

    @staticmethod
    def clear_and_copy_dir(src, dst):
        if os.path.exists(dst):
            shutil.rmtree(dst, ignore_errors=True)
        shutil.copytree(src, dst)

    @property
    def singularity_image_url(self):
        return 'https://s3.amazonaws.com/openstudio-builds/{ver}/OpenStudio-{ver}.{sha}-Singularity.simg'.format(
                    ver=self.os_version,
                    sha=self.os_sha
                )

    @property
    def singularity_image(self):
        # Check the project yaml specification - if the file does not exist do not silently allow for non-specified simg
        if 'sys_image_dir' in self.cfg.keys():
            sys_image_dir = self.cfg['sys_image_dir']
            sys_image = os.path.join(sys_image_dir, 'OpenStudio-{ver}.{sha}-Singularity.simg'.format(
                ver=self.os_version,
                sha=self.os_sha
            ))
            if os.path.isfile(sys_image):
                return sys_image
            else:
                raise RuntimeError('Unable to find singularity image specified in project file: `{}`'.format(sys_image))
        # Use the expected HPC environment default if not explicitly defined in the YAML
        sys_image = os.path.join(self.sys_image_dir, 'OpenStudio-{ver}.{sha}-Singularity.simg'.format(
            ver=self.os_version,
            sha=self.os_sha
        ))
        if os.path.isfile(sys_image):
            return sys_image
        # Otherwise attempt retrieval from AWS for the appropriate os_version and os_sha
        else:
            singularity_image_path = os.path.join(self.output_dir, 'openstudio.simg')
            if not os.path.isfile(singularity_image_path):
                logger.debug(f'Downloading singularity image: {self.singularity_image_url}')
                r = requests.get(self.singularity_image_url, stream=True)
                if r.status_code != requests.codes.ok:
                    logger.error('Unable to download simg file from OpenStudio releases S3 bucket.')
                    r.raise_for_status()
                with open(singularity_image_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                logger.debug('Downloaded singularity image to {}'.format(singularity_image_path))
            return singularity_image_path

    @property
    def weather_dir(self):
        weather_dir = os.path.join(self.output_dir, 'weather')
        if not os.path.exists(weather_dir):
            os.makedirs(weather_dir)
            self._get_weather_files()
        return weather_dir

    def run_batch(self, sampling_only=False):

        # Create simulation_output dir
        sim_out_ts_dir = pathlib.Path(self.output_dir) / 'results' / 'simulation_output' / 'timeseries'
        os.makedirs(sim_out_ts_dir, exist_ok=True)
        for i in range(0, len(self.cfg.get('upgrades', [])) + 1):
            os.makedirs(sim_out_ts_dir / f'up{i:02d}')

        # create destination_dir and copy housing_characteristics into it
        logger.debug("Copying housing characteristics")
        destination_dir = os.path.dirname(self.sampler.csv_path)
        if os.path.exists(destination_dir):
            shutil.rmtree(destination_dir)
        shutil.copytree(
            os.path.join(self.project_dir, 'housing_characteristics'),
            destination_dir
        )
        logger.debug("Housing characteristics copied.")

        # run sampling
        buildstock_csv_filename = self.sampler.run_sampling()

        # Hit the weather_dir API to make sure that creating the weather directory isn't a race condition in the array
        # jobs - this is rare but happens quasi-repeatably when lustre is really lagging
        _ = self.weather_dir

        if sampling_only:
            return

        # Determine the number of simulations expected to be executed
        df = pd.read_csv(buildstock_csv_filename, index_col=0)

        # find out how many buildings there are to simulate
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        # number of simulations is number of buildings * number of upgrades
        n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)

        # this is the number of simulations defined for this run as a "full job"
        #     number of simulations per job if we believe the .yml file n_jobs
        n_sims_per_job = math.ceil(n_sims / self.cfg[self.hpc_name]['n_jobs'])
        #     use more appropriate batch size in the case of n_jobs being much
        #     larger than we need, now that we know n_sims
        n_sims_per_job = max(n_sims_per_job, self.min_sims_per_job)

        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get('upgrades', []))))
        if not self.skip_baseline_sims:
            # create batches of simulations
            baseline_sims = zip(building_ids, itertools.repeat(None))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        else:
            all_sims = list(itertools.chain(upgrade_sims))
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        for i in itertools.count(1):
            batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
            if not batch:
                break
            logger.info('Queueing job {} ({} simulations)'.format(i, len(batch)))
            job_json_filename = os.path.join(self.output_dir, 'job{:03d}.json'.format(i))
            with open(job_json_filename, 'w') as f:
                json.dump({
                    'job_num': i,
                    'n_datapoints': n_datapoints,
                    'batch': batch,
                }, f, indent=4)

        # now queue them
        jobids = self.queue_jobs()

        # queue up post-processing to run after all the simulation jobs are complete
        if not get_bool_env_var('MEASURESONLY'):
            self.queue_post_processing(jobids)

    def run_job_batch(self, job_array_number):

        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / 'resources',
            self.local_buildstock_dir / 'resources'
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.buildstock_dir) / 'measures',
            self.local_buildstock_dir / 'measures'
        )
        if os.path.exists(pathlib.Path(self.buildstock_dir) / 'resources/hpxml-measures'):
            self.clear_and_copy_dir(
                pathlib.Path(self.buildstock_dir) / 'resources/hpxml-measures',
                self.local_buildstock_dir / 'resources/hpxml-measures'
            )
        self.clear_and_copy_dir(
            self.weather_dir,
            self.local_weather_dir
        )
        self.clear_and_copy_dir(
            pathlib.Path(self.output_dir) / 'housing_characteristics',
            self.local_housing_characteristics_dir
        )
        if os.path.exists(self.local_singularity_img):
            os.remove(self.local_singularity_img)
        shutil.copy2(self.singularity_image, self.local_singularity_img)

        # Run the job batch as normal
        job_json_filename = os.path.join(self.output_dir, 'job{:03d}.json'.format(job_array_number))
        with open(job_json_filename, 'r') as f:
            args = json.load(f)

        # trim the buildstock.csv file to only include rows for current batch. Helps speed up simulation
        logger.debug("Trimming buildstock.csv")
        building_ids = {x[0] for x in args['batch']}
        buildstock_csv_path = self.local_housing_characteristics_dir / 'buildstock.csv'
        valid_rows = []
        with open(buildstock_csv_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if int(row['Building']) in building_ids:
                    valid_rows.append(row)
        df = pd.DataFrame.from_records(valid_rows)
        df.to_csv(buildstock_csv_path, index=False)
        logger.debug(f"Buildstock.csv trimmed to {len(df)} rows.")

        traceback_file_path = self.local_output_dir / 'simulation_output' / f'traceback{job_array_number}.out'

        @delayed
        def run_building_d(i, upgrade_idx):
            try:
                return self.run_building(self.output_dir, self.cfg, args['n_datapoints'], i, upgrade_idx)
            except Exception:
                with open(traceback_file_path, 'a') as f:
                    txt = get_error_details()
                    txt = "\n" + "#" * 20 + "\n" + f"Traceback for building{i}\n" + txt
                    f.write(txt)
                    del txt
                upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1
                return {"building_id": i, "upgrade": upgrade_id}

        # Run the simulations, get the data_point_out.json info from each
        tick = time.time()
        with Parallel(n_jobs=-1, verbose=9) as parallel:
            dpouts = parallel(itertools.starmap(run_building_d, args['batch']))
        tick = time.time() - tick
        logger.info('Simulation time: {:.2f} minutes'.format(tick / 60.))

        # Save the aggregated dpouts as a json file
        lustre_sim_out_dir = pathlib.Path(self.results_dir) / 'simulation_output'
        results_json = lustre_sim_out_dir / f'results_job{job_array_number}.json.gz'
        logger.info(f'Writing results to {results_json}')
        with gzip.open(results_json, 'wt', encoding='utf-8') as f:
            json.dump(dpouts, f)

        # Compress simulation results
        simout_filename = lustre_sim_out_dir / f'simulations_job{job_array_number}.tar.gz'
        logger.info(f'Compressing simulation outputs to {simout_filename}')
        local_sim_out_dir = self.local_output_dir / 'simulation_output'
        subprocess.run(
            [
                'tar',
                'cf', str(simout_filename),
                '-I', 'pigz',
                '-C', str(local_sim_out_dir),
                '.'
            ],
            check=True
        )

        # copy the tracebacks if it exists
        if os.path.exists(traceback_file_path):
            shutil.copy2(traceback_file_path, lustre_sim_out_dir)

    @classmethod
    def run_building(cls, output_dir, cfg, n_datapoints, i, upgrade_idx=None):

        fs = LocalFileSystem()
        upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(cls.local_output_dir, 'simulation_output'))
        except SimulationExists as ex:
            sim_dir = ex.sim_dir
        else:
            # Generate the osw for this simulation
            osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)
            with open(os.path.join(sim_dir, 'in.osw'), 'w') as f:
                json.dump(osw, f, indent=4)

            # Copy other necessary stuff into the simulation directory
            dirs_to_mount = [
                os.path.join(cls.local_buildstock_dir, 'measures'),
                cls.local_weather_dir,
            ]

            # Build the command to instantiate and configure the singularity container the simulation is run inside
            local_resources_dir = cls.local_buildstock_dir / 'resources'
            args = [
                'singularity', 'exec',
                '--contain',
                '-e',
                '--pwd', '/var/simdata/openstudio',
                '-B', f'{sim_dir}:/var/simdata/openstudio',
                '-B', f'{local_resources_dir}:/lib/resources',
                '-B', f'{cls.local_housing_characteristics_dir}:/lib/housing_characteristics'
            ]
            runscript = [
                'ln -s /lib /var/simdata/openstudio/lib'
            ]
            for src in dirs_to_mount:
                container_mount = '/' + os.path.basename(src)
                args.extend(['-B', '{}:{}:ro'.format(src, container_mount)])
                container_symlink = os.path.join('/var/simdata/openstudio', os.path.basename(src))
                runscript.append('ln -s {} {}'.format(*map(shlex.quote, (container_mount, container_symlink))))

            if os.path.exists(os.path.join(cls.local_buildstock_dir, 'resources/hpxml-measures')):
                runscript.append('ln -s /resources /var/simdata/openstudio/resources')
                src = os.path.join(cls.local_buildstock_dir, 'resources/hpxml-measures')
                container_mount = '/resources/hpxml-measures'
                args.extend(['-B', '{}:{}:ro'.format(src, container_mount)])

            # Build the openstudio command that will be issued within the singularity container
            # If custom gems are to be used in the singularity container add extra bundle arguments to the cli command
            cli_cmd = 'openstudio run -w in.osw'
            if cfg.get('baseline', dict()).get('custom_gems', False):
                cli_cmd = 'openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems run -w in.osw --debug'
            if get_bool_env_var('MEASURESONLY'):
                cli_cmd += ' --measures_only'
            runscript.append(cli_cmd)
            args.extend([
                str(cls.local_singularity_img),
                'bash', '-x'
            ])
            logger.debug('\n'.join(map(str, args)))
            with open(os.path.join(sim_dir, 'singularity_output.log'), 'w') as f_out:
                try:
                    subprocess.run(
                        args,
                        check=True,
                        input='\n'.join(runscript).encode('utf-8'),
                        stdout=f_out,
                        stderr=subprocess.STDOUT,
                        cwd=cls.local_output_dir
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

                    # Clean up simulation directory
                    cls.cleanup_sim_dir(
                        sim_dir,
                        fs,
                        f'{output_dir}/results/simulation_output/timeseries',
                        upgrade_id,
                        i
                    )

        reporting_measures = cls.get_reporting_measures(cfg)
        dpout = postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, i)
        return dpout

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
        memory = self.cfg['eagle'].get('postprocessing', {}).get('node_memory_mb', 85248)
        print(f"Submitting job to {memory}MB memory nodes.")
        # Throw an error if the files already exist.
        if not upload_only:
            for subdir in ('parquet', 'results_csvs'):
                subdirpath = pathlib.Path(self.output_dir, 'results', subdir)
                if subdirpath.exists():
                    raise FileExistsError(f'{subdirpath} already exists. This means you may have run postprocessing already. If you are sure you want to rerun, delete that directory and try again.')  # noqa E501

        # Move old output logs and config to make way for new ones
        for filename in ('dask_scheduler.json', 'dask_scheduler.out', 'dask_workers.out', 'postprocessing.out'):
            filepath = pathlib.Path(self.output_dir, filename)
            if filepath.exists():
                last_mod_date = dt.datetime.fromtimestamp(os.path.getmtime(filepath))
                shutil.move(
                    filepath,
                    filepath.parent / f'{filepath.stem}_{last_mod_date:%Y%m%d%H%M}{filepath.suffix}'
                )

        env = {}
        env.update(os.environ)
        env['PROJECTFILE'] = self.project_filename
        env['MY_CONDA_ENV'] = os.environ['CONDA_PREFIX']
        env['OUT_DIR'] = self.output_dir
        env['UPLOADONLY'] = str(upload_only)
        env['MEMORY'] = str(memory)
        here = os.path.dirname(os.path.abspath(__file__))
        eagle_post_sh = os.path.join(here, 'eagle_postprocessing.sh')

        args = [
            'sbatch',
            '--account={}'.format(account),
            '--time={}'.format(walltime),
            '--export=PROJECTFILE,MY_CONDA_ENV,OUT_DIR,UPLOADONLY,MEMORY',
            '--job-name=bstkpost',
            '--output=postprocessing.out',
            '--nodes=1',
            ':',
            '--mem={}'.format(memory),
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
        if get_bool_env_var('DASKLOCALCLUSTER'):
            cluster = LocalCluster(local_directory='/data/dask-tmp')
            return Client(cluster)
        else:
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
    print(BuildStockBatchBase.LOGO)

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


@log_error_details()
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
        logger.debug("Starting postprocessing")
        # else, we might be in a post-processing step
        # Postprocessing should not have been scheduled if measures only or sampling only are run
        assert(not measures_only)
        assert(not sampling_only)
        if upload_only:
            batch.process_results(skip_combine=True, force_upload=True)
        else:
            batch.process_results()
    else:
        logger.debug("Kicking off batch")
        # default job_array_number == 0 task is to kick the whole BuildStock
        # process off, that is, to create samples and then create batch jobs
        # to run them
        batch.run_batch(sampling_only)


if __name__ == '__main__':
    main()
