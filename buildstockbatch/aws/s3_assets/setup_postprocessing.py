from setuptools import setup

setup(
    name='buildstockbatch-postprocessing',
    version='0.1',
    description='Just the stand alone postprocessing functions from Buildstock-Batch',
    py_modules=['postprocessing'],
    install_requires=[
        'dask[complete]',
        's3fs>=0.4.2,<0.5.0',
        'boto3',
        'pandas>=1.0.0,!=1.0.4',
        'pyarrow>=0.14.1',
    ]
)
