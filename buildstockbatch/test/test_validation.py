#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

This file contains the code required to test validation methods across

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""


import os
import pytest
import types
from buildstockbatch.eagle import EagleBatch
from buildstockbatch.localdocker import LocalDockerBatch
from buildstockbatch.base import BuildStockBatchBase

here = os.path.dirname(os.path.abspath(__file__))
example_yml_dir = os.path.join(here, 'test_inputs')


def test_base_validation_is_static():
    assert(isinstance(BuildStockBatchBase.validate_project, types.FunctionType))


def test_base_schema_validation_is_static():
    assert(isinstance(BuildStockBatchBase.validate_project_schema, types.FunctionType))


def test_eagle_validation_is_static():
    assert(isinstance(EagleBatch.validate_project, types.FunctionType))


def test_local_docker_validation_is_static():
    assert(isinstance(LocalDockerBatch.validate_project, types.FunctionType))


def test_schema_validation_passes():
    assert(BuildStockBatchBase.validate_project_schema(os.path.join(example_yml_dir, 'complete-schema.yml')))


def test_missing_required_key_fails():
    expected_failures = [
        os.path.join(example_yml_dir, 'missing-required-schema.yml'),
        os.path.join(example_yml_dir, 'missing-nested-required-schema.yml')
    ]
    for expected_failure in expected_failures:
        with pytest.raises(Exception):
            assert BuildStockBatchBase.validate_project_schema(expected_failure)
