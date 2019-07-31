import requests

from buildstockbatch.hpc import HPCBatchBase


def test_singularity_image_download_url():
    url = HPCBatchBase.singularity_image_url()
    r = requests.head(url)
    assert r.status_code == requests.codes.ok
