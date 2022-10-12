import os
import requests
import re
from unittest.mock import patch
import yaml
import pytest

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
        assert r1.status_code == 401
        m = re.search(r'realm="(.+?)"', r1.headers['Www-Authenticate'])
        authurl = m.group(1)
        m = re.search(r'service="(.+?)"', r1.headers['Www-Authenticate'])
        service = m.group(1)
        r2 = requests.get(authurl, params={
            'service': service,
            'scope': f'repository:{docker_image}:pull'
        })
        assert r2.ok
        token = r2.json()['token']
        r3 = requests.head(
            f'{baseurl}{docker_image}/manifests/{docker_tag}',
            headers={'Authorization': f'Bearer {token}'}
        )
        assert r3.ok


@pytest.mark.skip(reason="See if it works without this test running")
def test_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()

    # Add custom_gems to the project file
    with open(project_filename, 'r') as f:
        cfg = yaml.safe_load(f)
    cfg['baseline']['custom_gems'] = True
    with open(project_filename, 'w') as f:
        yaml.dump(cfg, f)

    buildstock_directory = cfg['buildstock_directory']

    LocalDockerBatch(project_filename)

    bundle_install_log_path = os.path.join(buildstock_directory,
                                           'resources',
                                           '.custom_gems',
                                           'bundle_install_output.log')
    assert os.path.exists(bundle_install_log_path)
    os.remove(bundle_install_log_path)

    gem_list_log_log_path = os.path.join(buildstock_directory,
                                         'resources',
                                         '.custom_gems',
                                         'openstudio_gem_list_output.log')
    assert os.path.exists(gem_list_log_log_path)
    os.remove(gem_list_log_log_path)
