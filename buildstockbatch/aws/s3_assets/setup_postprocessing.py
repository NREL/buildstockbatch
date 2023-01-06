from setuptools import setup

setup(
    name='buildstockbatch-postprocessing',
    version='0.1',
    description='Just the stand alone postprocessing functions from Buildstock-Batch',
    py_modules=['postprocessing'],
    # install_requires=[
    #     'dask[complete]>=2022.10.0',
    #     's3fs[boto3]',
    #     'pandas',
    #     'pyarrow',
    #     'numpy'
    # ]
)
