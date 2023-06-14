# -*- coding: utf-8 -*-

"""
buildstockbatch.localdocker
~~~~~~~~~~~~~~~
This object contains the code required for execution of local batch simulations

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import argparse
from dask.distributed import Client, LocalCluster
from distutils.version import StrictVersion
import functools
from fsspec.implementations.local import LocalFileSystem
import gzip
import itertools
from joblib import Parallel, delayed
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tarfile

from buildstockbatch.base import BuildStockBatchBase, SimulationExists
from buildstockbatch import postprocessing
from buildstockbatch.utils import log_error_details, ContainerRuntime, read_csv
from buildstockbatch.__version__ import __version__ as bsb_version

logger = logging.getLogger(__name__)


class LocalBatch(BuildStockBatchBase):

    CONTAINER_RUNTIME = ContainerRuntime.LOCAL_OPENSTUDIO

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self._weather_dir = None

        # Create simulation_output dir
        sim_out_ts_dir = pathlib.Path(self.results_dir, 'simulation_output', 'timeseries')
        for i in range(0, len(self.cfg.get('upgrades', [])) + 1):
            (sim_out_ts_dir / f'up{i:02d}').mkdir(exist_ok=True, parents=True)

        # Install custom gems to a volume that will be used by all workers
        if self.cfg.get('baseline', {}).get('custom_gems', False):

            # Check that the Gemfile exists
            local_gemfile_path = os.path.join(self.buildstock_dir, 'resources', 'Gemfile')
            if not os.path.exists(local_gemfile_path):
                print(f'local_gemfile_path = {local_gemfile_path}')
                raise AttributeError('baseline:custom_gems = True, but did not find Gemfile in /resources directory')

            # Change executables based on operating system
            gem_exe = 'gem'
            bundler_exe = 'bundle'
            if os.name == 'nt':
                gem_exe = 'gem.cmd'
                bundler_exe = 'bundle.cmd'

            # Check the bundler version
            proc_output = subprocess.run(
                [gem_exe, 'list'],
                check=True,
                capture_output=True,
                text=True
            )
            ruby_bundler_versions = self._get_gem_versions_from_gem_list('bundler', proc_output.stdout)
            proc_output = subprocess.run(
                [self.openstudio_exe(), 'gem_list'],
                check=True,
                capture_output=True,
                text=True
            )
            openstudio_bundler_versions = self._get_gem_versions_from_gem_list('bundler', proc_output.stdout)
            common_bundler_versions = set(ruby_bundler_versions).intersection(openstudio_bundler_versions)
            if common_bundler_versions:
                # Use the most recent bundler that is in both
                openstudio_bundler_version = sorted(common_bundler_versions, key=StrictVersion)[-1]
            else:
                # Install the bundler that's in openstudio
                openstudio_bundler_version = sorted(openstudio_bundler_versions, key=StrictVersion)[-1]
                subprocess.run(
                    [gem_exe, 'install', 'bundler', '-v', openstudio_bundler_version],
                    check=True
                )

            # Clear and create the buildstock/.custom_gems dir to store gems
            gems_install_path = pathlib.Path(self.buildstock_dir, '.custom_gems')
            if not gems_install_path.exists():
                gems_install_path.mkdir(parents=True)

            # Run bundler to install the custom gems
            copied_gemfile_path = gems_install_path / "Gemfile"
            shutil.copy2(local_gemfile_path, copied_gemfile_path)
            logger.debug(f'Installing custom gems to {gems_install_path}')
            proc_output = subprocess.run(
                [
                    bundler_exe, f"_{openstudio_bundler_version}_",
                    "install",
                    '--path', str(gems_install_path),
                    '--gemfile', str(copied_gemfile_path),
                    "--without", "native_ext",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            with open(gems_install_path / 'bundle_install_output.log', 'wb') as f_out:
                f_out.write(proc_output.stdout)
            proc_output.check_returncode()

            # Report out custom gems loaded by OpenStudio CLI
            proc_output = subprocess.run(
                [
                    self.openstudio_exe(),
                    "--bundle", str(copied_gemfile_path),
                    "--bundle_path", str(gems_install_path),
                    "--bundle_without", "native_ext",
                    "gem_list"
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT
            )
            gem_list_log = gems_install_path / 'openstudio_gem_list_output.log'
            with open(gem_list_log, 'wb') as f_out:
                f_out.write(proc_output.stdout)
            logger.debug(f'Review custom gems list at: {gem_list_log}')
            proc_output.check_returncode()

    @staticmethod
    def _get_gem_versions_from_gem_list(gem_name, gem_list_txt):
        gem_versions_txt = re.search(
            rf"^{gem_name} \((.*)\)",
            gem_list_txt,
            re.MULTILINE
        ).group(1).strip()
        gem_versions = re.findall(r"[\d\.]+", gem_versions_txt)
        return gem_versions

    @classmethod
    def validate_project(cls, project_file):
        super(cls, cls).validate_project(project_file)
        # LocalBatch specific code goes here
        assert cls.validate_openstudio_path(project_file)

    @property
    def weather_dir(self):
        if self._weather_dir is None:
            self._weather_dir = os.path.join(self.buildstock_dir, 'weather')
            self._get_weather_files()
        return self._weather_dir

    @classmethod
    def run_building(cls, buildstock_dir, weather_dir, results_dir, measures_only,
                     n_datapoints, cfg, i, upgrade_idx=None):

        upgrade_id = 0 if upgrade_idx is None else upgrade_idx + 1

        try:
            sim_id, sim_dir = cls.make_sim_dir(i, upgrade_idx, os.path.join(results_dir, 'simulation_output'))
        except SimulationExists:
            return
        sim_path = pathlib.Path(sim_dir)
        buildstock_path = pathlib.Path(buildstock_dir)

        # Make symlinks to project and buildstock stuff
        (sim_path / 'measures').symlink_to(buildstock_path / 'measures', target_is_directory=True)
        (sim_path / 'lib').symlink_to(buildstock_path / "lib", target_is_directory=True)
        (sim_path / 'weather').symlink_to(weather_dir, target_is_directory=True)
        hpxml_measures_path = buildstock_path / 'resources' / 'hpxml-measures'
        if hpxml_measures_path.exists():
            resources_path = sim_path / 'resources'
            resources_path.mkdir()
            (resources_path / 'hpxml-measures').symlink_to(hpxml_measures_path, target_is_directory=True)
        else:
            resources_path = None

        osw = cls.create_osw(cfg, n_datapoints, sim_id, building_id=i, upgrade_idx=upgrade_idx)

        with open(sim_path / 'in.osw', 'w') as f:
            json.dump(osw, f, indent=4)

        run_cmd = [
            cls.openstudio_exe(),
            'run',
            '-w', 'in.osw',
        ]

        if cfg.get('baseline', dict()).get('custom_gems', False):
            custom_gem_dir = buildstock_path / '.custom_gems'
            run_cmd = [
                cls.openstudio_exe(),
                '--bundle', str(custom_gem_dir / 'Gemfile'),
                '--bundle_path', str(custom_gem_dir),
                '--bundle_without', 'native_ext',
                'run', '-w', 'in.osw',
                '--debug'
            ]

        if measures_only:
            idx = run_cmd.index('run') + 1
            run_cmd.insert(idx, '--measures_only')

        env_vars = {}
        env_vars.update(os.environ)
        env_vars['BUILDSTOCKBATCH_VERSION'] = bsb_version

        proc_output = subprocess.run(
            run_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env_vars,
            cwd=sim_dir,
        )
        with open(sim_path / 'openstudio_output.log', 'wb') as f_out:
            f_out.write(proc_output.stdout)

        fs = LocalFileSystem()
        cls.cleanup_sim_dir(
            sim_dir,
            fs,
            f"{results_dir}/simulation_output/timeseries",
            upgrade_id,
            i
        )

        # Clean up symlinks
        for directory in ('measures', 'lib', 'weather'):
            (sim_path / directory).unlink()
        if resources_path:
            (resources_path / 'hpxml-measures').unlink()
            resources_path.rmdir()

        # Read data_point_out.json
        reporting_measures = cls.get_reporting_measures(cfg)
        dpout = postprocessing.read_simulation_outputs(fs, reporting_measures, sim_dir, upgrade_id, i)
        return dpout

    def run_batch(self, n_jobs=None, measures_only=False, sampling_only=False):
        buildstock_csv_filename = self.sampler.run_sampling()

        if sampling_only:
            return

        # Copy files to lib dir in buildstock root
        buildstock_path = pathlib.Path(self.buildstock_dir)
        project_path = pathlib.Path(self.project_dir)
        lib_path = pathlib.Path(self.buildstock_dir, 'lib')
        if lib_path.exists():
            shutil.rmtree(lib_path)
        lib_path.mkdir()
        shutil.copytree(buildstock_path / "resources", lib_path / "resources")
        shutil.copytree(project_path / "housing_characteristics", lib_path / "housing_characteristics")

        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)

        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        run_building_d = functools.partial(
            delayed(self.run_building),
            self.buildstock_dir,
            self.weather_dir,
            self.results_dir,
            measures_only,
            n_datapoints,
            self.cfg
        )
        upgrade_sims = []
        for i in range(len(self.cfg.get('upgrades', []))):
            upgrade_sims.append(map(functools.partial(run_building_d, upgrade_idx=i), building_ids))
        if not self.skip_baseline_sims:
            baseline_sims = map(run_building_d, building_ids)
            all_sims = itertools.chain(baseline_sims, *upgrade_sims)
        else:
            all_sims = itertools.chain(*upgrade_sims)
        if n_jobs is None:
            n_jobs = -1
        dpouts = Parallel(n_jobs=n_jobs, verbose=10)(all_sims)

        shutil.rmtree(lib_path)

        sim_out_path = pathlib.Path(self.results_dir, 'simulation_output')

        results_job_json_filename = sim_out_path / 'results_job0.json.gz'
        with gzip.open(results_job_json_filename, 'wt', encoding='utf-8') as f:
            json.dump(dpouts, f)
        del dpouts

        sim_out_tarfile_name = sim_out_path / 'simulations_job0.tar.gz'
        logger.debug(f'Compressing simulation outputs to {sim_out_tarfile_name}')
        with tarfile.open(sim_out_tarfile_name, 'w:gz') as tarf:
            for dirname in os.listdir(sim_out_path):
                if re.match(r'up\d+', dirname) and (sim_out_path / dirname).is_dir():
                    tarf.add(sim_out_path / dirname, arcname=dirname)
                    shutil.rmtree(sim_out_path / dirname)

    @property
    def output_dir(self):
        return self.results_dir

    @property
    def results_dir(self):
        results_dir = self.cfg.get(
            'output_directory',
            os.path.join(self.project_dir, 'localResults')
        )
        results_dir = self.path_rel_to_projectfile(results_dir)
        if not os.path.isdir(results_dir):
            os.makedirs(results_dir)
        return results_dir

    def get_dask_client(self):
        cluster = LocalCluster(local_directory=os.path.join(self.results_dir, 'dask-tmp'))
        return Client(cluster)


@log_error_details()
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
    print(BuildStockBatchBase.LOGO)
    parser.add_argument('project_filename')
    parser.add_argument(
        '-j',
        type=int,
        help='Number of parallel simulations. Default: all cores.',
        default=None
    )
    parser.add_argument(
        '-m', '--measures_only',
        action='store_true',
        help='Only apply the measures, but don\'t run simulations. Useful for debugging.'
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--postprocessonly',
                       help='Only do postprocessing, useful for when the simulations are already done',
                       action='store_true')
    group.add_argument('--uploadonly',
                       help='Only upload to S3, useful when postprocessing is already done. Ignores the '
                       'upload flag in yaml', action='store_true')
    group.add_argument('--validateonly', help='Only validate the project YAML file and references. Nothing is executed',
                       action='store_true')
    group.add_argument('--samplingonly', help='Run the sampling only.',
                       action='store_true')
    args = parser.parse_args()
    if not os.path.isfile(args.project_filename):
        raise FileNotFoundError(f'The project file {args.project_filename} doesn\'t exist')

    # Validate the project, and in case of the --validateonly flag return True if validation passes
    LocalBatch.validate_project(args.project_filename)
    if args.validateonly:
        return True
    batch = LocalBatch(args.project_filename)
    if not (args.postprocessonly or args.uploadonly or args.validateonly):
        batch.run_batch(n_jobs=args.j, measures_only=args.measures_only, sampling_only=args.samplingonly)
    if args.measures_only or args.samplingonly:
        return
    if args.uploadonly:
        batch.process_results(skip_combine=True, force_upload=True)
    else:
        batch.process_results()


if __name__ == '__main__':
    main()
