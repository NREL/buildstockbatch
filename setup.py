#!/usr/bin/env python

from codecs import open
import os
import setuptools
from setuptools.command.test import test as testing_cmd
import sys


class PyTest(testing_cmd):
    user_options = [('pytest-args=', 'a', "Arguments to pass into py.test")]

    def initialize_options(self):
        testing_cmd.initialize_options(self)
        try:
            from multiprocessing import cpu_count
            self.pytest_args = ['-n', str(cpu_count()), '--boxed']
        except (ImportError, NotImplementedError):
            self.pytest_args = ['-n', '1', '--boxed']

    def finalize_options(self):
        testing_cmd.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


here = os.path.abspath(os.path.dirname(__file__))
metadata = {}

with open(os.path.join(here, 'buildstockbatch', '__version__.py'), 'r', encoding='utf-8') as f:
    exec(f.read(), metadata)

with open('README.md', 'r', 'utf-8') as f:
    readme = f.read()

setuptools.setup(
    name=metadata['__title__'],
    version=metadata['__version__'],
    author=metadata['__author__'],
    author_email=metadata['__author_email__'],
    description=metadata['__description__'],
    long_description=readme,
    long_description_content_type='text/markdown',
    url=metadata['__url__'],
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    package_data={
        'buildstockbatch': ['*.sh', 'schemas/*.yaml'],
        '': ['LICENSE']
    },
    install_requires=[
        'pyyaml',
        'requests',
        'numpy>=1.11',
        'pandas',
        'joblib',
        'pyarrow>=0.10.0',
        'dask[complete]>=2.1.0',
        'docker',
        'boto3',
        'fs',
        'fs-s3fs',
        'yamale'
    ],
    extras_require={
        'dev': [
            'pytest>=2.8.0',
            'codecov',
            'Sphinx',
            'sphinx_rtd_theme',
            'sphinx-autobuild',
            'sphinxcontrib-programoutput',
            'flake8',
            'coverage',
            'rope',
            'doc8'
        ]
    },
    entry_points={
        'console_scripts': [
            'buildstock_docker=buildstockbatch.localdocker:main',
            'buildstock_eagle=buildstockbatch.eagle:user_cli',
        ]
    },
    cmdclass={'test': PyTest},
    license='BSD-3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)
