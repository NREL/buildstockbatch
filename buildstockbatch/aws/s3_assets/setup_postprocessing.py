from setuptools import setup

setup(
    name='buildstockbatch-postprocessing',
    version='0.1',
    description='Just the stand along postprocessing functions from Buildstock-Batch',
    py_modules=['postprocessing'],
    install_requires=[
        'dask[complete]',
        'fs-s3fs',
        'boto3',
        'pandas',
        'pyarrow>=0.14.1',
    ]
)
