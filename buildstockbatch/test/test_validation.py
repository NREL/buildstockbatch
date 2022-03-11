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
from buildstockbatch.base import BuildStockBatchBase, ValidationError
from unittest.mock import patch
from testfixtures import LogCapture
import logging

here = os.path.dirname(os.path.abspath(__file__))
example_yml_dir = os.path.join(here, 'test_inputs')


def filter_logs(logs, level):
    filtered_logs = ''
    for record in logs.records:
        if record.levelname == level:
            filtered_logs += record.msg
    return filtered_logs


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


@pytest.mark.parametrize("project_file,expected", [
    (os.path.join(example_yml_dir, 'enforce-schema-xor.yml'), ValidationError),
    (os.path.join(example_yml_dir, 'enforce-schema-xor-and-passes.yml'), True),
])
def test_xor_violations_fail(project_file, expected):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, 'validate_options_lookup', lambda _: True):
        if expected is not True:
            with pytest.raises(expected):
                BuildStockBatchBase.validate_xor_nor_schema_keys(project_file)
        else:
            assert(BuildStockBatchBase.validate_xor_nor_schema_keys(project_file))


@pytest.mark.parametrize("project_file,expected", [
    (os.path.join(example_yml_dir, 'missing-required-schema.yml'), ValueError),
    (os.path.join(example_yml_dir, 'missing-nested-required-schema.yml'), ValueError),
    (os.path.join(example_yml_dir, 'enforce-schema-xor.yml'), ValidationError),
    (os.path.join(example_yml_dir, 'complete-schema.yml'), True),
    (os.path.join(example_yml_dir, 'minimal-schema.yml'), True)
])
def test_validation_integration(project_file, expected):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, 'validate_options_lookup', lambda _: True), \
            patch.object(BuildStockBatchBase, 'validate_measure_references', lambda _: True), \
            patch.object(BuildStockBatchBase, 'validate_workflow_generator', lambda _: True):
        if expected is not True:
            with pytest.raises(expected):
                BuildStockBatchBase.validate_project(project_file)
        else:
            assert(BuildStockBatchBase.validate_project(project_file))


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-bad-2.yml')
])
def test_bad_reference_scenario(project_file):

    with LogCapture(level=logging.INFO) as logs:
        BuildStockBatchBase.validate_reference_scenario(project_file)
        warning_logs = filter_logs(logs, 'WARNING')
        assert "non-existing upgrade' does not match " in warning_logs


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-good-2.yml')
])
def test_good_reference_scenario(project_file):
    with LogCapture(level=logging.INFO) as logs:
        assert BuildStockBatchBase.validate_reference_scenario(project_file)
        warning_logs = filter_logs(logs, 'WARNING')
        error_logs = filter_logs(logs, 'ERROR')
        assert warning_logs == ''
        assert error_logs == ''


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-bad-2.yml')
])
def test_bad_measures(project_file):

    with LogCapture(level=logging.INFO) as logs:
        try:
            BuildStockBatchBase.validate_workflow_generator(project_file)
        except ValidationError as er:
            er = str(er)
            warning_logs = filter_logs(logs, 'WARNING')
            assert "Required argument calendar_year for" in warning_logs
            assert "ReportingMeasure2 does not exist" in er
            assert "Wrong argument value type for begin_day_of_month" in er
            assert "Found unexpected argument key output_variable" in er
            assert "Found unexpected argument value Huorly" in er
            assert "Fixed(1)" in er
            assert "Required argument include_enduse_subcategories" in er
            assert "Found unexpected argument key include_enduse_subcategory" in er

        else:
            raise Exception("measures_and_arguments was supposed to raise ValueError for"
                            " enforce-validate-measures-bad.yml")


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-good-2.yml'),
])
def test_good_measures(project_file):
    with LogCapture(level=logging.INFO) as logs:
        assert BuildStockBatchBase.validate_workflow_generator(project_file)
        warning_logs = filter_logs(logs, 'WARNING')
        error_logs = filter_logs(logs, 'ERROR')
        assert warning_logs == ''
        assert error_logs == ''


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-wrong-path.yml'),
])
def test_bad_path_options_validation(project_file):
    with pytest.raises(FileNotFoundError):
        BuildStockBatchBase.validate_options_lookup(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-good.yml'),
    os.path.join(example_yml_dir, 'enforce-validate-options-good-2.yml'),
])
def test_good_options_validation(project_file):
    assert BuildStockBatchBase.validate_options_lookup(project_file)
    assert BuildStockBatchBase.validate_postprocessing_spec(project_file)


@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-bad.yml'),
    os.path.join(example_yml_dir, 'enforce-validate-options-bad-2.yml'),
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
        assert "1941s" in er
        assert "Option name empty" in er
        assert "Insulation Slat" in er
        assert "Vintage|1960s|Vintage|1960s" in er
        assert "Vintage|1960s||Vintage|1940s&&Vintage|1980s" in er

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

@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-measures-good.yml'),
])
def test_good_measures_validation(project_file):
    assert BuildStockBatchBase.validate_measure_references(project_file)

@pytest.mark.parametrize("project_file", [
    os.path.join(example_yml_dir, 'enforce-validate-options-bad-2.yml'),
])
def test_bad_postprocessing_spec_validation(project_file):
    try:
        BuildStockBatchBase.validate_postprocessing_spec(project_file)
    except ValidationError as er:
        er = str(er)
        assert "bad_partition_column" in er
    else:
        raise Exception("validate_options was supposed to raise ValueError for enforce-validate-options-bad-2.yml")