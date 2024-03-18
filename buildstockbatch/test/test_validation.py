#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

This file contains the code required to test validation methods across

:author: Noel Merket, Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import inspect
import os
import pytest
import types
import tempfile
import json
import pathlib
from buildstockbatch.hpc import EagleBatch, SlurmBatch, KestrelBatch
from buildstockbatch.aws.aws import AwsBatch
from buildstockbatch.local import LocalBatch
from buildstockbatch.base import BuildStockBatchBase, ValidationError
from buildstockbatch.test.shared_testing_stuff import (
    resstock_directory,
    resstock_required,
)
from buildstockbatch.utils import get_project_configuration
from unittest.mock import patch
from testfixtures import LogCapture
from yamale.yamale_error import YamaleError
import logging
import yaml

here = os.path.dirname(os.path.abspath(__file__))
example_yml_dir = os.path.join(here, "test_inputs")
resources_dir = os.path.join(here, "test_inputs", "test_openstudio_buildstock", "resources")


def filter_logs(logs, level):
    filtered_logs = ""
    for record in logs.records:
        if record.levelname == level:
            filtered_logs += record.msg
    return filtered_logs


def test_base_validation_is_classmethod():
    assert inspect.ismethod(BuildStockBatchBase.validate_project)


def test_base_schema_validation_is_static():
    assert isinstance(BuildStockBatchBase.validate_project_schema, types.FunctionType)


def test_eagle_validation_is_classmethod():
    assert inspect.ismethod(EagleBatch.validate_project)


def test_local_docker_validation_is_classmethod():
    assert inspect.ismethod(LocalBatch.validate_project)


def test_aws_batch_validation_is_static():
    assert isinstance(AwsBatch.validate_project, types.FunctionType)
    assert isinstance(AwsBatch.validate_dask_settings, types.FunctionType)


def test_complete_schema_passes_validation():
    assert BuildStockBatchBase.validate_project_schema(os.path.join(example_yml_dir, "complete-schema.yml"))


def test_minimal_schema_passes_validation():
    assert BuildStockBatchBase.validate_project_schema(os.path.join(example_yml_dir, "minimal-schema.yml"))


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "missing-required-schema.yml"),
        os.path.join(example_yml_dir, "missing-nested-required-schema.yml"),
    ],
)
def test_missing_required_key_fails(project_file):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, "validate_options_lookup", lambda _: True):
        with pytest.raises(ValueError):
            BuildStockBatchBase.validate_project_schema(project_file)


@pytest.mark.parametrize(
    "project_file,expected",
    [
        (os.path.join(example_yml_dir, "enforce-schema-xor.yml"), ValidationError),
        (os.path.join(example_yml_dir, "enforce-schema-xor-and-passes.yml"), True),
    ],
)
def test_xor_violations_fail(project_file, expected):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, "validate_options_lookup", lambda _: True):
        if expected is not True:
            with pytest.raises(expected):
                BuildStockBatchBase.validate_xor_nor_schema_keys(project_file)
        else:
            assert BuildStockBatchBase.validate_xor_nor_schema_keys(project_file)


@pytest.mark.parametrize(
    "project_file, base_expected, eagle_expected",
    [
        (
            os.path.join(example_yml_dir, "missing-required-schema.yml"),
            ValueError,
            ValueError,
        ),
        (
            os.path.join(example_yml_dir, "missing-nested-required-schema.yml"),
            ValueError,
            ValueError,
        ),
        (
            os.path.join(example_yml_dir, "enforce-schema-xor.yml"),
            ValidationError,
            ValidationError,
        ),
        (os.path.join(example_yml_dir, "complete-schema.yml"), True, True),
        (os.path.join(example_yml_dir, "minimal-schema.yml"), True, ValidationError),
    ],
)
def test_validation_integration(project_file, base_expected, eagle_expected):
    # patch the validate_options_lookup function to always return true for this case
    with patch.object(BuildStockBatchBase, "validate_options_lookup", lambda _: True), patch.object(
        BuildStockBatchBase, "validate_measure_references", lambda _: True
    ), patch.object(BuildStockBatchBase, "validate_workflow_generator", lambda _: True), patch.object(
        BuildStockBatchBase, "validate_postprocessing_spec", lambda _: True
    ), patch.object(
        SlurmBatch, "validate_apptainer_image_hpc", lambda _: True
    ):
        for cls, expected in [
            (BuildStockBatchBase, base_expected),
            (EagleBatch, eagle_expected),
        ]:
            if expected is not True:
                with pytest.raises(expected):
                    cls.validate_project(project_file)
            else:
                assert cls.validate_project(project_file)


@pytest.mark.parametrize(
    "project_file",
    [os.path.join(example_yml_dir, "enforce-validate-measures-bad-2.yml")],
)
def test_bad_reference_scenario(project_file):
    with LogCapture(level=logging.INFO) as logs:
        BuildStockBatchBase.validate_reference_scenario(project_file)
        warning_logs = filter_logs(logs, "WARNING")
        assert "non-existing upgrade' does not match " in warning_logs


@pytest.mark.parametrize(
    "project_file",
    [os.path.join(example_yml_dir, "enforce-validate-measures-good-2.yml")],
)
def test_good_reference_scenario(project_file):
    with LogCapture(level=logging.INFO) as logs:
        assert BuildStockBatchBase.validate_reference_scenario(project_file)
        warning_logs = filter_logs(logs, "WARNING")
        error_logs = filter_logs(logs, "ERROR")
        assert warning_logs == ""
        assert error_logs == ""


@pytest.mark.parametrize(
    "project_file",
    [os.path.join(example_yml_dir, "enforce-validate-measures-bad-2.yml")],
)
def test_bad_measures(project_file):
    with LogCapture(level=logging.INFO) as _:
        try:
            BuildStockBatchBase.validate_workflow_generator(project_file)
        except (ValidationError, YamaleError) as er:
            er = str(er)
            assert "'1.5' is not a int" in er
            assert "'huorly' not in ('none', 'timestep', 'hourly', 'daily', 'monthly')" in er
        else:
            raise Exception(
                "measures_and_arguments was supposed to raise ValidationError for" " enforce-validate-measures-bad.yml"
            )


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-measures-good-2.yml"),
        os.path.join(example_yml_dir, "enforce-validate-measures-good-2-with-anchors.yml"),
    ],
)
def test_good_measures(project_file):
    with LogCapture(level=logging.INFO) as logs:
        assert BuildStockBatchBase.validate_workflow_generator(project_file)
        warning_logs = filter_logs(logs, "WARNING")
        error_logs = filter_logs(logs, "ERROR")
        assert warning_logs == ""
        assert error_logs == ""


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-options-wrong-path.yml"),
    ],
)
def test_bad_path_options_validation(project_file):
    with pytest.raises(FileNotFoundError):
        BuildStockBatchBase.validate_options_lookup(project_file)


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-options-good.yml"),
        os.path.join(example_yml_dir, "enforce-validate-options-good-2.yml"),
    ],
)
def test_good_options_validation(project_file):
    assert BuildStockBatchBase.validate_options_lookup(project_file)
    assert BuildStockBatchBase.validate_postprocessing_spec(project_file)


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-options-bad.yml"),
        os.path.join(example_yml_dir, "enforce-validate-options-bad-2.yml"),
    ],
)
def test_bad_options_validation(project_file):
    try:
        BuildStockBatchBase.validate_options_lookup(project_file)
    except ValidationError as er:
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
        assert "Wall Insulation: '*' cannot pass arguments to measure." in er
        assert "Wall Insulation: '*' cannot be mixed with other options" in er
        assert "Ceiling Insulation: '*' cannot be mixed with other options" in er
        assert "Floor Insulation: '*' cannot be mixed with other options" in er

    else:
        raise Exception("validate_options was supposed to raise ValueError for enforce-validate-options-bad.yml")


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-measures-good.yml"),
    ],
)
def test_good_measures_validation(project_file):
    assert BuildStockBatchBase.validate_measure_references(project_file)


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-measures-bad.yml"),
    ],
)
def test_bad_measures_validation(project_file):
    try:
        BuildStockBatchBase.validate_measure_references(project_file)
    except ValidationError as er:
        er = str(er)
        assert "Measure directory" in er
        assert "not found" in er
        assert "ResidentialConstructionsUnfinishedBasement" in er
        assert "ResidentialConstructionsFinishedBasement" in er

    else:
        raise Exception(
            "validate_measure_references was supposed to raise ValueError for " "enforce-validate-measures-bad.yml"
        )


@pytest.mark.parametrize(
    "project_file",
    [
        os.path.join(example_yml_dir, "enforce-validate-options-bad-2.yml"),
    ],
)
def test_bad_postprocessing_spec_validation(project_file):
    try:
        BuildStockBatchBase.validate_postprocessing_spec(project_file)
    except ValidationError as er:
        er = str(er)
        assert "bad_partition_column" in er
    else:
        raise Exception("validate_options was supposed to raise ValidationError for enforce-validate-options-bad-2.yml")


@pytest.mark.parametrize("project_file", [os.path.join(example_yml_dir, "enforce-validate-options-good.yml")])
def test_logic_validation_fail(project_file):
    try:
        BuildStockBatchBase.validate_logic(project_file)
    except ValidationError as er:
        er = str(er)
        assert "'Insulation Wall' occurs 2 times in a 'not' block" in er
        assert "'Vintage' occurs 2 times in a 'and' block" in er
        assert "'Vintage' occurs 2 times in a '&&' block" in er
    else:
        raise Exception("validate_options was supposed to raise ValidationError for enforce-validate-options-good.yml")


@pytest.mark.parametrize(
    "project_file",
    [os.path.join(example_yml_dir, "enforce-validate-options-all-good.yml")],
)
def test_logic_validation_pass(project_file):
    BuildStockBatchBase.validate_logic(project_file)


@resstock_required
def test_number_of_options_apply_upgrade():
    proj_filename = resstock_directory / "project_national" / "national_upgrades.yml"
    cfg = get_project_configuration(str(proj_filename))
    cfg["upgrades"][-1]["options"] = cfg["upgrades"][-1]["options"] * 10
    cfg["upgrades"][0]["options"][0]["costs"] = cfg["upgrades"][0]["options"][0]["costs"] * 5
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = pathlib.Path(tmpdir)
        new_proj_filename = tmppath / "project.yml"
        with open(new_proj_filename, "w") as f:
            json.dump(cfg, f)
        with pytest.raises(ValidationError):
            LocalBatch.validate_number_of_options(str(new_proj_filename))


@resstock_required
def test_validate_resstock_or_comstock_version(mocker):
    # Set the version to a 'really old' one so we trigger the version check error
    mocker.patch("buildstockbatch.base.bsb_version", "1.0.0")
    proj_filename = resstock_directory / "project_national" / "national_upgrades.yml"
    with pytest.raises(ValidationError):
        BuildStockBatchBase.validate_resstock_or_comstock_version(str(proj_filename))


def test_dask_config():
    orig_filename = os.path.join(example_yml_dir, "minimal-schema.yml")
    cfg = get_project_configuration(orig_filename)
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg["aws"] = {
            "dask": {
                "scheduler_cpu": 1024,
                "scheduler_memory": 2048,
                "worker_cpu": 1024,
                "worker_memory": 2048,
                "n_workers": 1,
            }
        }
        test1_filename = os.path.join(tmpdir, "test1.yml")
        with open(test1_filename, "w") as f:
            json.dump(cfg, f)
        AwsBatch.validate_dask_settings(test1_filename)
        cfg["aws"]["dask"]["scheduler_memory"] = 9 * 1024
        test2_filename = os.path.join(tmpdir, "test2.yml")
        with open(test2_filename, "w") as f:
            json.dump(cfg, f)
        with pytest.raises(ValidationError, match=r"between 2048 and 8192"):
            AwsBatch.validate_dask_settings(test2_filename)
        cfg["aws"]["dask"]["scheduler_memory"] = 8 * 1024
        cfg["aws"]["dask"]["worker_memory"] = 1025
        test3_filename = os.path.join(tmpdir, "test3.yml")
        with open(test3_filename, "w") as f:
            json.dump(cfg, f)
        with pytest.raises(ValidationError, match=r"needs to be a multiple of 1024"):
            AwsBatch.validate_dask_settings(test3_filename)


def test_validate_eagle_output_directory():
    minimal_yml = pathlib.Path(example_yml_dir, "minimal-schema.yml")
    with pytest.raises(ValidationError, match=r"must be in /scratch or /projects"):
        EagleBatch.validate_output_directory_eagle(str(minimal_yml))
    with tempfile.TemporaryDirectory() as tmpdir:
        dirs_to_try = [
            "/scratch/username/out_dir",
            "/projects/projname/out_dir",
            "/lustre/eaglefs/scratch/username/out_dir",
            "/lustre/eaglefs/projects/projname/out_dir",
        ]
        for output_directory in dirs_to_try:
            with open(minimal_yml, "r") as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
            cfg["output_directory"] = output_directory
            temp_yml = pathlib.Path(tmpdir, "temp.yml")
            with open(temp_yml, "w") as f:
                yaml.dump(cfg, f, Dumper=yaml.SafeDumper)
            EagleBatch.validate_output_directory_eagle(str(temp_yml))


def test_validate_kestrel_output_directory():
    minimal_yml = pathlib.Path(example_yml_dir, "minimal-schema.yml")
    with pytest.raises(ValidationError, match=r"must be in /scratch or /projects"):
        KestrelBatch.validate_output_directory_kestrel(str(minimal_yml))
    with tempfile.TemporaryDirectory() as tmpdir:
        dirs_to_try = [
            "/scratch/username/out_dir",
            "/projects/projname/out_dir",
            "/kfs2/scratch/username/out_dir",
            "/kfs3/projects/projname/out_dir",
        ]
        for output_directory in dirs_to_try:
            with open(minimal_yml, "r") as f:
                cfg = yaml.load(f, Loader=yaml.SafeLoader)
            cfg["output_directory"] = output_directory
            temp_yml = pathlib.Path(tmpdir, "temp.yml")
            with open(temp_yml, "w") as f:
                yaml.dump(cfg, f, Dumper=yaml.SafeDumper)
            KestrelBatch.validate_output_directory_kestrel(str(temp_yml))


def test_validate_apptainer_image():
    minimal_yml = pathlib.Path(example_yml_dir, "minimal-schema.yml")
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(minimal_yml, "r") as f:
            cfg = yaml.load(f, Loader=yaml.SafeLoader)
        cfg["sys_image_dir"] = tmpdir
        temp_yml = pathlib.Path(tmpdir, "temp.yml")
        with open(temp_yml, "w") as f:
            yaml.dump(cfg, f, Dumper=yaml.SafeDumper)
        with pytest.raises(ValidationError, match=r"Could not find apptainer image: .+\.sif or .+\.simg"):
            SlurmBatch.validate_apptainer_image_hpc(str(temp_yml))
        for ext in ["Apptainer.sif", "Singularity.simg"]:
            filename = pathlib.Path(
                tmpdir, f"OpenStudio-{SlurmBatch.DEFAULT_OS_VERSION}.{SlurmBatch.DEFAULT_OS_SHA}-{ext}"
            )
            filename.touch()
            SlurmBatch.validate_apptainer_image_hpc(str(temp_yml))
            filename.unlink()


def test_validate_sampler_good_buildstock(basic_residential_project_file):
    project_filename, _ = basic_residential_project_file(
        {
            "sampler": {
                "type": "precomputed",
                "args": {"sample_file": str(os.path.join(resources_dir, "buildstock_good.csv"))},
            }
        }
    )
    assert BuildStockBatchBase.validate_sampler(project_filename)


def test_validate_sampler_bad_buildstock(basic_residential_project_file):
    project_filename, _ = basic_residential_project_file(
        {
            "sampler": {
                "type": "precomputed",
                "args": {"sample_file": str(os.path.join(resources_dir, "buildstock_bad.csv"))},
            }
        }
    )
    try:
        BuildStockBatchBase.validate_sampler(project_filename)
    except ValidationError as er:
        er = str(er)
        assert "Option 1940-1950 in column Vintage of buildstock_csv is not available in options_lookup.tsv" in er
        assert "Option TX in column State of buildstock_csv is not available in options_lookup.tsv" in er
        assert "Option nan in column Insulation Wall of buildstock_csv is not available in options_lookup.tsv" in er
        assert "Column Insulation in buildstock_csv is not available in options_lookup.tsv" in er
        assert "Column ZipPlusCode in buildstock_csv is not available in options_lookup.tsv" in er
    else:
        raise Exception("validate_options was supposed to raise ValidationError for enforce-validate-options-good.yml")
