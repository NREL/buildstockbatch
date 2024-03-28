#!/usr/bin/env python

from codecs import open
import os
import setuptools


here = os.path.abspath(os.path.dirname(__file__))
metadata = {}

with open(os.path.join(here, "buildstockbatch", "__version__.py"), "r", encoding="utf-8") as f:
    exec(f.read(), metadata)

with open("README.md", "r", "utf-8") as f:
    readme = f.read()

gcp_requires = [
    "gcsfs",
    "google-cloud-artifact-registry",
    "google-cloud-batch",
    "google-cloud-compute",
    "google-cloud-run",
    "google-cloud-storage",
    "tqdm",
]

aws_requires = [
    "dask-cloudprovider[aws]",
]


setuptools.setup(
    name=metadata["__title__"],
    version=metadata["__version__"],
    author=metadata["__author__"],
    author_email=metadata["__author_email__"],
    description=metadata["__description__"],
    long_description=readme,
    long_description_content_type="text/markdown",
    url=metadata["__url__"],
    packages=setuptools.find_packages(),
    python_requires=">=3.8",
    package_data={"buildstockbatch": ["*.sh", "schemas/*.yaml"], "": ["LICENSE"]},
    install_requires=[
        "pyyaml",
        "requests",
        "numpy",
        "pandas>=2",
        "joblib",
        "pyarrow",
        "dask[complete]>=2022.10.0",
        "docker",
        "s3fs[boto3]",
        "fsspec",
        "yamale",
        "ruamel.yaml",
        "lxml",
        "semver",
        "tqdm",
    ],
    extras_require={
        "dev": [
            "pytest",
            "pytest-mock",
            "pytest-cov",
            "testfixtures",
            "Sphinx",
            "sphinx_rtd_theme>=1.1.0",
            "sphinx-autobuild",
            "sphinxcontrib-programoutput",
            "sphinx_paramlinks",
            "changelog",
            "flake8",
            "black~=24.0",
            "rope",
            "doc8",
            "pre-commit",
        ]
        + gcp_requires
        + aws_requires,
        "gcp": gcp_requires,
        "aws": aws_requires,
    },
    entry_points={
        "console_scripts": [
            "buildstock_local=buildstockbatch.local:main",
            "buildstock_eagle=buildstockbatch.hpc:eagle_cli",
            "buildstock_kestrel=buildstockbatch.hpc:kestrel_cli",
            "buildstock_gcp=buildstockbatch.gcp.gcp:main",
            "buildstock_aws=buildstockbatch.aws.aws:main",
        ]
    },
    license="BSD-3",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
