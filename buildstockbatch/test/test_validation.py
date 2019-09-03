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
from unittest.mock import patch

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
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, 'validate_options_lookup', lambda _: True):
        with pytest.raises(ValueError):
            BuildStockBatchBase.validate_project_schema(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-schema-xor-missing.yml'),
    os.path.join(example_yml_dir, 'enforce-schema-xor-nested.yml'),
    os.path.join(example_yml_dir, 'enforce-schema-xor.yml')
])
def test_xor_violations_fail(project_file):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, 'validate_options_lookup', lambda _: True):
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
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, 'validate_options_lookup', lambda _: True), \
            patch.object(BuildStockBatchBase, 'validate_measure_references', lambda _: True):
        if expected is not True:
            with pytest.raises(expected):
                BuildStockBatchBase.validate_project(project_file)
        else:
            assert(BuildStockBatchBase.validate_project(project_file))


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-wrong-path.yml'),
])
def test_bad_path_options_validation(project_file):
    with pytest.raises(FileNotFoundError):
        BuildStockBatchBase.validate_options_lookup(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-good.yml'),
])
def test_good_options_validation(project_file):
    assert BuildStockBatchBase.validate_options_lookup(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-bad.yml'),
])
def test_bad_options_validation(project_file):
    try:
        BuildStockBatchBase.validate_options_lookup(project_file)
    except ValueError as er:
        er = str(er)
        assert "Insulation Slab(Good) Option" in er
        assert "Insulation Unfinished&Basement" in er
        assert "Insulation Finished|Basement" in er
        assert "Extra Argument" in er
        assert "Invalid Option" in er
        assert "Insulation Wall|Good Option||" in er
        assert " 1980s" in er
        assert "Vintage|1941s" in er
        assert "Option name empty" in er
        assert "Insulation Slat" in er
        assert "Vintage|1960s|Vintage|1960s" in er
        assert "Vintage|1960s||Vintage|1940s&&Vintage|1980s" in er
        assert "Invalid Parameter" in er

    else:
        raise Exception("validate_options was supposed to raise ValueError for enforce-validate-options-bad.yml")


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-good.yml'),
])
def test_good_measures_validation(project_file):
    assert BuildStockBatchBase.validate_measure_references(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-bad.yml'),
])
def test_bad_measures_validation(project_file):
    try:
        BuildStockBatchBase.validate_measure_references(project_file)
    except ValueError as er:
        er = str(er)
        assert "Measure directory" in er
        assert "not found" in er
        assert "ResidentialConstructionsUnfinishedBasement" in er
        assert "ResidentialConstructionsFinishedBasement" in er

    else:
        raise Exception("validate_measure_references was supposed to raise ValueError for "
                        "enforce-validate-measures-bad.yml")
