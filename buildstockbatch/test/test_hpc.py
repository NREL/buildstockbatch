import requests

from buildstockbatch.hpc import HPCBatchBase


def test_singularity_image_download_url():
    url = HPCBatchBase.singularity_image_url()
    r = requests.head(url)
    assert r.status_code == requests.codes.ok

    # To check for a docker image...
    # Use a bearer tokean as at
    # https://docs.docker.com/registry/spec/auth/token/
    # https://docs.docker.com/registry/spec/api/

