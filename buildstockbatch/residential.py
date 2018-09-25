# -*- coding: utf-8 -*-

"""
buildstockbatch.residential
~~~~~~~~~~~~~~~
This module contains commercial building specific implementations for use in other classes

:author: Noel Merket
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

from copy import deepcopy
import datetime as dt
import logging
import os
import shutil
import subprocess
import time

logger = logging.getLogger(__name__)


def res_create_osw(cls, sim_id, cfg, i, upgrade_idx):
    osw = {
        'id': sim_id,
        'steps': [
            {
                'measure_dir_name': 'BuildExistingModel',
                'arguments': {
                    'building_id': i,
                    'workflow_json': 'measure-info.json',
                    'sample_weight': cfg['baseline']['n_buildings_represented'] / cfg['baseline']['n_datapoints']
                }
            }
        ],
        'created_at': dt.datetime.now().isoformat(),
        'measure_paths': [
            'measures'
        ],
        'seed_file': 'seeds/EmptySeedModel.osm',
        'weather_file': 'weather/Placeholder.epw'
    }

    osw['steps'].extend(cfg['baseline'].get('measures', []))

    osw['steps'].extend([
        {
            'measure_dir_name': 'BuildingCharacteristicsReport',
            'arguments': {}
        },
        {
            'measure_dir_name': 'SimulationOutputReport',
            'arguments': {}
        },
        {
            'measure_dir_name': 'ServerDirectoryCleanup',
            'arguments': {}
        }
    ])

    if upgrade_idx is not None:
        measure_d = cfg['upgrades'][upgrade_idx]
        apply_upgrade_measure = {
            'measure_dir_name': 'ApplyUpgrade',
            'arguments': {
                'upgrade_name': measure_d['upgrade_name'],
                'run_measure': 1
            }
        }
        for opt_num, option in enumerate(measure_d['options'], 1):
            apply_upgrade_measure['arguments']['option_{}'.format(opt_num)] = option['option']
            if 'lifetime' in option:
                apply_upgrade_measure['arguments']['option_{}_lifetime'.format(opt_num)] = option['lifetime']
            if 'apply_logic' in option:
                apply_upgrade_measure['arguments']['option_{}_apply_logic'.format(opt_num)] = \
                    cls.make_apply_logic_arg(option['apply_logic'])
            for cost_num, cost in enumerate(option['costs'], 1):
                for arg in ('value', 'multiplier'):
                    if arg not in cost:
                        continue
                    apply_upgrade_measure['arguments']['option_{}_cost_{}_{}'.format(opt_num, cost_num, arg)] = \
                        cost[arg]
        if 'package_apply_logic' in measure_d:
            apply_upgrade_measure['package_apply_logic'] = cls.make_apply_logic_arg(measure_d['package_apply_logic'])

        osw['steps'].insert(1, apply_upgrade_measure)

    if 'timeseries_csv_export' in cfg:
        timeseries_measure = {
            'measure_dir_name': 'TimeseriesCSVExport',
            'arguments': deepcopy(cfg['timeseries_csv_export'])
        }
        timeseries_measure['arguments']['output_variables'] = \
            ','.join(cfg['timeseries_csv_export']['output_variables'])
        osw['steps'].insert(-1, timeseries_measure)


def res_run_peregrine_sampling(cls, n_datapoints):
    if n_datapoints is None:
        n_datapoints = cls.cfg['baseline']['n_datapoints']
    logging.debug('Sampling, n_datapoints={}'.format(n_datapoints))
    args = [
        'singularity',
        'exec',
        '--contain',
        '--home', cls.buildstock_dir,
        cls.singularity_image,
        'ruby',
        'resources/run_sampling.rb',
        '-p', cls.cfg['project_directory'],
        '-n', str(n_datapoints),
        '-o', 'buildstock.csv'
    ]
    subprocess.run(args, check=True, env=os.environ, cwd=cls.output_dir)
    destination_dir = os.path.join(cls.output_dir, 'housing_characteristics')
    if os.path.exists(destination_dir):
        shutil.rmtree(destination_dir)
    shutil.copytree(
        os.path.join(cls.project_dir, 'housing_characteristics'),
        destination_dir
    )
    assert (os.path.isdir(destination_dir))
    shutil.move(
        os.path.join(cls.buildstock_dir, 'resources', 'buildstock.csv'),
        destination_dir
    )
    return os.path.join(destination_dir, 'buildstock.csv')


def res_run_local_sampling(cls, n_datapoints):
    if n_datapoints is None:
        n_datapoints = cls.cfg['baseline']['n_datapoints']
    logger.debug('Sampling, n_datapoints={}'.format(n_datapoints))
    tick = time.time()
    container_output = cls.docker_client.containers.run(
        cls.docker_image(),
        [
            'ruby',
            'resources/run_sampling.rb',
            '-p', cls.cfg['project_directory'],
            '-n', str(n_datapoints),
            '-o', 'buildstock.csv'
        ],
        remove=True,
        volumes={
            cls.buildstock_dir: {'bind': '/var/simdata/openstudio', 'mode': 'rw'}
        },
        name='buildstock_sampling'
    )
    tick = time.time() - tick
    for line in container_output.decode('utf-8').split('\n'):
        logger.debug(line)
    logger.debug('Sampling took {:.1f} seconds'.format(tick))
    destination_filename = os.path.join(cls.project_dir, 'housing_characteristics', 'buildstock.csv')
    if os.path.exists(destination_filename):
        os.remove(destination_filename)
    shutil.move(
        os.path.join(cls.buildstock_dir, 'resources', 'buildstock.csv'),
        destination_filename
    )
    return destination_filename
