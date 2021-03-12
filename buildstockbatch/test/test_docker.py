import os
import requests
import re
from unittest.mock import patch

from buildstockbatch.base import BuildStockBatchBase

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
