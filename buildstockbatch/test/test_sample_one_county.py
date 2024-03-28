"""Tests the sample_one_county script."""

import csv
import os
import pytest
import tempfile
from unittest.mock import MagicMock

from buildstockbatch.sample_one_county import SampleOnly, residential_quota


def test_sample_one_county_script(mocker):
    buildstock_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_inputs/test_openstudio_buildstock")
    county = "G0100110"
    PUMA = "G01002400"

    def check_input_files(project_dir):
        # Check values in `ASHRAE IECC Climate Zone 2004.tsv`
        zone_tsv = os.path.join(project_dir, "housing_characteristics", "ASHRAE IECC Climate Zone 2004.tsv")
        with open(zone_tsv) as f:
            reader = csv.reader(f, delimiter="\t")
            headers = next(reader)
            assert headers == [
                "Option=1A",
                "Option=2A",
                "Option=2B",
                "Option=3A",
                "Option=3B",
                "Option=3C",
                "Option=4A",
                "Option=4B",
                "Option=4C",
                "Option=5A",
                "Option=5B",
                "Option=6A",
                "Option=6B",
                "Option=7A",
                "Option=7AK",
                "Option=7B",
                "Option=8AK",
                "source_count",
                "source_weight",
                "sampling_probability",
            ]
            row = next(reader)
            # Expected climate zone is 3A
            assert row == 3 * ["0"] + ["1"] + 16 * ["0"]
            with pytest.raises(StopIteration):
                next(reader)

        # Check values in `County and PUMA.tsv`
        county_tsv = os.path.join(project_dir, "housing_characteristics", "County and PUMA.tsv")
        with open(county_tsv) as f:
            reader = csv.reader(f, delimiter="\t")
            headers = next(reader)
            for row in reader:
                if row[0] == "3A":
                    for h, v in zip(headers[1:], row[1:]):
                        if h == f"Option={county}, {PUMA}":
                            assert v == "1"
                        else:
                            assert v == "0"

    with tempfile.TemporaryDirectory(prefix="test_sample_") as tmpdir:
        output_dir = tmpdir

        def mock_init(self, parent, n):
            # Mock out the actual sampling, since that depends on ResStock's sampling code
            def run_sampling():
                # Create an empty file in place of the expected output
                with open(os.path.join(parent.project_dir, "housing_characteristics", "buildstock.csv"), "w"):
                    pass

                # Check that the input files were modified correctly
                check_input_files(parent.project_dir)

            self.run_sampling = run_sampling

        mocker.patch.object(residential_quota.ResidentialQuotaSampler, "__init__", mock_init)

        s = SampleOnly(buildstock_dir, tmpdir)
        s.run_sampler(county, PUMA, 10)

        assert os.listdir(output_dir) == ["buildstock_G0100110_G01002400_10.csv"]
