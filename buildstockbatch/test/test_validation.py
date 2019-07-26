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


def test_complete_schema_passes_validation():
    assert(BuildStockBatchBase.validate_project_schema(os.path.join(example_yml_dir, 'complete-schema.yml')))


def test_minimal_schema_passes_validation():
    assert(BuildStockBatchBase.validate_project_schema(os.path.join(example_yml_dir, 'minimal-schema.yml')))


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'missing-required-schema.yml'),
    os.path.join(example_yml_dir, 'missing-nested-required-schema.yml')
])
def test_missing_required_key_fails(project_file):
    with pytest.raises(ValueError):
        BuildStockBatchBase.validate_project_schema(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-schema-xor-missing.yml'),
    os.path.join(example_yml_dir, 'enforce-schema-xor-nested.yml'),
    os.path.join(example_yml_dir, 'enforce-schema-xor.yml')
])
def test_xor_violations_fail(project_file):
    with pytest.raises(ValueError):
        BuildStockBatchBase.validate_xor_schema_keys(project_file)


@pytest.mark.parametrize("project_file,expected", [
    (os.path.join(example_yml_dir, 'missing-required-schema.yml'), ValueError),
    (os.path.join(example_yml_dir, 'missing-nested-required-schema.yml'), ValueError),
    (os.path.join(example_yml_dir, 'enforce-schema-xor-missing.yml'), ValueError),
    (os.path.join(example_yml_dir, 'enforce-schema-xor-nested.yml'), ValueError),
    (os.path.join(example_yml_dir, 'enforce-schema-xor.yml'), ValueError),
    (os.path.join(example_yml_dir, 'complete-schema.yml'), True),
    (os.path.join(example_yml_dir, 'minimal-schema.yml'), True)
])
def test_validation_integration(project_file, expected):
    if expected is not True:
        with pytest.raises(expected):
            BuildStockBatchBase.validate_project(project_file)
    else:
        assert(BuildStockBatchBase.validate_project(project_file))
