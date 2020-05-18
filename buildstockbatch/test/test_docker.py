import os
import pandas as pd
import pytest
import requests
import re
from unittest.mock import patch
import yaml

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.localdocker import LocalDockerBatch

here = os.path.dirname(os.path.abspath(__file__))


def test_docker_image_exists_on_docker_hub(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()
    # Use a BuildStockBatchBase instance to get the version of OpenStudio
    # because instantiating a LocalDockerBatch fails to connect
    # with the docker website in the testing context for some reason.
    with patch.object(BuildStockBatchBase, 'weather_dir', None):
        bsb = BuildStockBatchBase(project_filename)
        docker_image = 'nrel/openstudio'
        docker_tag = bsb.os_version
        baseurl = 'https://registry.hub.docker.com/v2/'
        r1 = requests.get(baseurl)
        assert(r1.status_code == 401)
        m = re.search(r'realm="(.+?)"', r1.headers['Www-Authenticate'])
        authurl = m.group(1)
        m = re.search(r'service="(.+?)"', r1.headers['Www-Authenticate'])
        service = m.group(1)
        r2 = requests.get(authurl, params={
            'service': service,
            'scope': f'repository:{docker_image}:pull'
        })
        assert(r2.ok)
        token = r2.json()['token']
        r3 = requests.head(
            f'{baseurl}{docker_image}/manifests/{docker_tag}',
            headers={'Authorization': f'Bearer {token}'}
        )
        assert(r3.ok)


def test_provide_buildstock_csv(basic_residential_project_file):
    buildstock_csv = os.path.join(here, 'buildstock.csv')
    df = pd.read_csv(buildstock_csv)
    project_filename, results_dir = basic_residential_project_file({
        'baseline': {
            'n_datapoints': 10,
            'n_buildings_represented': 80000000,
            'sampling_algorithm': 'precomputed',
            'precomputed_sample': buildstock_csv
        }
    })
    with patch.object(LocalDockerBatch, 'weather_dir', None), \
            patch.object(LocalDockerBatch, 'results_dir', results_dir):
        bsb = LocalDockerBatch(project_filename)
        sampling_output_csv = bsb.run_sampling()
        df2 = pd.read_csv(sampling_output_csv)
        pd.testing.assert_frame_equal(df, df2)

    # Test n_datapoints do not match
    with open(project_filename, 'r') as f:
        cfg = yaml.safe_load(f)
    cfg['baseline']['n_datapoints'] = 100
    with open(project_filename, 'w') as f:
        yaml.dump(cfg, f)

    with patch.object(LocalDockerBatch, 'weather_dir', None), \
            patch.object(LocalDockerBatch, 'results_dir', results_dir):
        with pytest.raises(RuntimeError, match=r'does not match the number of rows in'):
            LocalDockerBatch(project_filename).run_sampling()

    # Test file missing
    with open(project_filename, 'r') as f:
        cfg = yaml.safe_load(f)
    cfg['baseline']['precomputed_sample'] = os.path.join(here, 'non_existant_file.csv')
    with open(project_filename, 'w') as f:
        yaml.dump(cfg, f)

    with patch.object(LocalDockerBatch, 'weather_dir', None), \
            patch.object(LocalDockerBatch, 'results_dir', results_dir):
        with pytest.raises(FileNotFoundError):
            LocalDockerBatch(project_filename).run_sampling()
