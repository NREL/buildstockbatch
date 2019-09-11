# -*- coding: utf-8 -*-

"""
buildstockbatch.hpc
~~~~~~~~~~~~~~~
This is the base class for high performance computing environments

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import functools
import itertools
from joblib import delayed, Parallel
import json
import logging as logging_
import math
import os
import pandas as pd
import random
import requests
import shlex
import shutil
import subprocess
import time

from .base import BuildStockBatchBase, SimulationExists
from .sampler import ResidentialSingularitySampler, CommercialSobolSampler

logger = logging_.getLogger(__name__)


class HPCBatchBase(BuildStockBatchBase):

    sys_image_dir = None
    hpc_name = None
    min_sims_per_job = None

    def __init__(self, project_filename):
        super().__init__(project_filename)
        output_dir = self.output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        logger.debug('Output directory = {}'.format(output_dir))

        if self.stock_type == 'residential':
            self.sampler = ResidentialSingularitySampler(
                self.singularity_image,
                self.output_dir,
                self.cfg,
                self.buildstock_dir,
                self.project_dir,
                subproject_directories = self.subproject_directories
            )
            self.buildstock_dir = self.sampler.buildstock_dir
        elif self.stock_type == 'commercial':
            sampling_algorithm = self.cfg['baseline'].get('sampling_algorithm', 'sobol')
            if sampling_algorithm == 'sobol':
                self.sampler = CommercialSobolSampler(
                    self.output_dir,
                    self.cfg,
                    self.buildstock_dir,
                    self.project_dir
                )
            else:
                raise NotImplementedError('Sampling algorithem "{}" is not implemented.'.format(sampling_algorithm))
        else:
            raise KeyError('stock_type = "{}" is not valid'.format(self.stock_type))

    @property
    def output_dir(self):
        raise NotImplementedError

    @classmethod
    def singularity_image_url(cls):
        return 'https://s3.amazonaws.com/openstudio-builds/{ver}/OpenStudio-{ver}.{sha}-Singularity.simg'.format(
                    ver=cls.OS_VERSION,
                    sha=cls.OS_SHA
                )

    @property
    def singularity_image(self):
        sys_image = os.path.join(self.sys_image_dir, 'OpenStudio-{ver}.{sha}-Singularity.simg'.format(
            ver=self.OS_VERSION,
            sha=self.OS_SHA
        ))
        if os.path.isfile(sys_image):
            return sys_image
        else:
            singularity_image_path = os.path.join(self.output_dir, 'openstudio.simg')
            if not os.path.isfile(singularity_image_path):
                logger.debug('Downloading singularity image')
                r = requests.get(self.singularity_image_url(), stream=True)
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

    @property
    def results_dir(self):
        results_dir = os.path.join(self.output_dir, 'results')
        assert(os.path.isdir(results_dir))
        return results_dir

    def run_batch(self):

        # create destination_dir and copy housing_characteristics into it
        destination_dir = os.path.dirname(self.sampler.csv_path)
        if os.path.exists(destination_dir):
            shutil.rmtree(destination_dir)
        shutil.copytree(
            os.path.join(self.project_dir, 'housing_characteristics'),
            destination_dir
        )

        # run sampling
        #   NOTE: If a buildstock_csv is provided, the BuildStockBatch
        #   constructor ensures that 'downselect' not in self.cfg and
        #   run_sampling simply copies that .csv to the correct location if
        #   necessary and returns the path
        if 'downselect' in self.cfg:
            # if there is a downselect section in the yml,
            # BuildStockBatchBase.downselect calls run_sampling and does
            # additional processing before and after
            buildstock_csv_filename = self.downselect()
        else:
            # otherwise just the plain sampling process needs to be run
            buildstock_csv_filename = self.run_sampling()

        # read the results
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
                    'batch': batch,
                }, f, indent=4)

        # now queue them
        jobids = self.queue_jobs()

        # queue up post-processing to run after all the simulation jobs are complete
        self.queue_post_processing(jobids)

    def run_job_batch(self, job_array_number):
        """
        Uses joblib to run_building in parallel
        """
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
        logger.info('Simulation time: {:.2f} minutes'.format(tick / 60.))

    @classmethod
    def run_building(cls, project_dir, buildstock_dir, weather_dir, output_dir, singularity_image, cfg, i,
                     upgrade_idx=None):

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(output_dir, 'simulation_output'))
        except SimulationExists:
            return

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
            '-e',
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
            singularity_image,
            'bash', '-x'
        ])
        logger.debug(' '.join(args))
        with open(os.path.join(sim_dir, 'singularity_output.log'), 'w') as f_out:
            try:
                subprocess.run(
                    args,
                    check=True,
                    input='\n'.join(runscript).encode('utf-8'),
                    stdout=f_out,
                    stderr=subprocess.STDOUT,
                    cwd=output_dir
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

                cls.cleanup_sim_dir(sim_dir)

        return sim_dir

    def get_dask_client(self):
        raise NotImplementedError

    def queue_jobs(self, array_ids=None):
        raise NotImplementedError

    def queue_post_processing(self, after_jobids):
        raise NotImplementedError

    @staticmethod
    def validate_project(project_file):
        return super(HPCBatchBase, HPCBatchBase).validate_project(project_file)
