"""Runs the residental quota sampler for a single county+PUMA.

Usage:
    python3 sample_one_county.py --help

    python3 sample_one_county.py G1900030 G19001800 100,200 path/to/resstock path/to/output_dir

        - Generates two files where every building has county=G1900030 and PUMA=G19001800:
            path/to/output_dir/buildstock_G1900030_G19001800_100.csv with 100 samples
            path/to/output_dir/buildstock_G1900030_G19001800_200.csv with 200 samples

Methodology:
    This modifies the conditional probability distributions from the standard ResStock national project
    to create a sample limited to a single county+PUMA. (For example, the selected location may normally
    be used for 1% of buildings in a national sample, but we update it to get 100% of buildings while
    every other location gets 0%.)

    To do this, we modify two files:
    - ASHRAE IECC Climate Zone 2004.tsv
        - Make 100% of the samples fall into the climate zone of the selected location.
    - County and PUMA.tsv
        - Make 100% of samples (within the chosen climate zone) fall into the selected county + PUMA

    All other housing characteristics are downstream of these (or don't depend on them) and are unchanged.

Assumptions:
    This logic is only guaranteed to work for the current ResStock national project. Other changes
    to the dependencies between the variables can break it!

    In particular, this code assumes:
        - ASHRAE climate zone has no dependencies
        - County and PUMA depends only on the ASHRAE climate zone
        - Each County+PUMA fall entirely in one climate zone
"""
import argparse
import csv
import os
import shutil
import tempfile

from buildstockbatch.utils import ContainerRuntime
from sampler import residential_quota


class SampleOnly:
    CONTAINER_RUNTIME = ContainerRuntime.DOCKER

    def __init__(self, buildstock_dir, output_dir):
        # Sampler uses this to find the sampling scripts
        self.buildstock_dir = os.path.abspath(buildstock_dir)

        # ResStock national project. Could use a different project, but `County and PUMA.tsv` and
        # `ASHRAE IECC Climate Zone 2004.tsv` must exist in the expected format.
        self.project_dir = os.path.join(self.buildstock_dir, "project_national")

        # Directory containing the conditional probability distributions we plan to modify
        self.housing_characteristics_dir = os.path.join(self.project_dir, "housing_characteristics")
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    @property
    def docker_image(self):
        return "nrel/openstudio:{}".format(self.os_version)

    @property
    def os_version(self):
        return "3.7.0"

    @property
    def project_filename(self):
        """Sampler expects this property to exist, but it can be None."""
        return None

    def get_climate_zone(self, county, PUMA):
        """Given a county and PUMA, find the climate zone that contains them.

        :param county: GISJOIN ID of county (e.g. "G1900030")
        :param PUMA: GISJOIN ID of PUMA (e.g. "G19001800")

        :return: Climate zone string (e.g. "3A")
        """
        with open(os.path.join(self.housing_characteristics_dir, "County and PUMA.tsv")) as f:
            reader = csv.reader(f, delimiter="\t")
            headers = next(reader)
            # Index of the column with the county and PUMA we're looking for.
            try:
                location_col = headers.index(f"Option={county}, {PUMA}")
            except ValueError as e:
                raise ValueError(f"Could not find 'Option={county}, {PUMA}' column in 'County and PUMA.tsv'") from e

            zone = None
            for row in reader:
                # Skip comments
                if row[0].strip()[0] == "#":
                    continue

                # Find the zone with a non-zero chance of producing this county + PUMA
                if row[location_col] != "0":
                    if zone:
                        raise ValueError(f"Found multiple climate zones for {county}, {PUMA}")
                    zone = row[0]

            if not zone:
                raise ValueError(f"No climate zone found for {county}, {PUMA}")
            return zone

    def run_sampler(self, county, PUMA, n_samples):
        """
        Create the requested number of buildings, all contained in the given county and PUMA.

        This function:
            - Updates the conditional probability distributions for climate zone and county + PUMA.
            - Runs the ResidentialQuotaSampler.
            - Renames and copies the resulting building.csv file into the output directory.

        :param county: GISJOIN ID of county (e.g. "G1900030")
        :param PUMA: GISJOIN ID of PUMA (e.g. "G19001800")
        :param n_samples: Number of building samples to produce.
        """

        climate_zone = self.get_climate_zone(county, PUMA)
        # Create a new copy of the probability distribution TSV files, so we can change them without
        # affecting the originals.
        with tempfile.TemporaryDirectory(prefix="sampling_", dir=self.buildstock_dir) as tmpdir:
            temp_housing_characteristics_dir = os.path.join(tmpdir, "housing_characteristics")
            shutil.copytree(self.housing_characteristics_dir, temp_housing_characteristics_dir)

            # Update climate zone TSV
            climate_zone_filename = "ASHRAE IECC Climate Zone 2004.tsv"
            zone_tsv = os.path.join(self.housing_characteristics_dir, climate_zone_filename)
            new_zone_tsv = os.path.join(temp_housing_characteristics_dir, climate_zone_filename)
            with open(zone_tsv) as old_f:
                reader = csv.reader(old_f, delimiter="\t")
                with open(new_zone_tsv, "w") as new_f:
                    writer = csv.writer(new_f, delimiter="\t")
                    headers = next(reader)
                    writer.writerow(headers)

                    # This file has a single row of probabilities, which we replace with 0s and a single 1.
                    zone_header = f"Option={climate_zone}"
                    writer.writerow(["1" if header == zone_header else "0" for header in headers])

            # Update county + PUMA TSV
            county_filename = "County and PUMA.tsv"
            county_tsv = os.path.join(self.housing_characteristics_dir, county_filename)
            new_county_tsv = os.path.join(temp_housing_characteristics_dir, county_filename)
            with open(county_tsv) as old_f:
                reader = csv.reader(old_f, delimiter="\t")
                with open(new_county_tsv, "w") as new_f:
                    writer = csv.writer(new_f, delimiter="\t")
                    headers = next(reader)
                    writer.writerow(headers)

                    # First value in headers lists the climate zone dependency -
                    # just use the others, which list the County+PUMA options.
                    assert headers[0] == "Dependency=ASHRAE IECC Climate Zone 2004"
                    headers = headers[1:]
                    for row in reader:
                        # Skip comments
                        if row[0].strip()[0] == "#":
                            continue

                        elif row[0] == climate_zone:
                            # Replace probabilities with 1 for our selected location and 0s everywhere else.
                            county_header = f"Option={county}, {PUMA}"
                            writer.writerow(
                                [row[0]] + ["1" if headers[i] == county_header else "0" for i, v in enumerate(row[1:])]
                            )

                        else:
                            # Leave other climate zones unchanged - they won't be used anyway.
                            writer.writerow(row)

            self.cfg = {"project_directory": os.path.basename(tmpdir)}
            self.project_dir = tmpdir

            # Note: Must create sampler after all instances vars exist, because it makes a copy of this object.
            sampler = residential_quota.ResidentialQuotaSampler(self, n_samples)
            sampler.run_sampling()

            # Copy results from temp dir to output dir
            shutil.copy(
                os.path.join(temp_housing_characteristics_dir, "buildstock.csv"),
                os.path.join(self.output_dir, f"buildstock_{county}_{PUMA}_{n_samples}.csv"),
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("county", help="County GISJOIN ID - https://www.nhgis.org/geographic-crosswalks#geog-ids")
    parser.add_argument("PUMA", help="PUMA GISJOIN ID")
    parser.add_argument("n_samples", help="Comma-separated list of samples sizes to generate")
    parser.add_argument("buildstock_dir", help="Path to the ResStock directory (expected to contain project_national)")
    parser.add_argument(
        "output_dir",
        default=".",
        nargs="?",
        help="Optional path where output should be written. Defaults to the current directory.",
    )
    args = parser.parse_args()

    assert (
        len(args.county) == 8 and args.county[0] == "G"
    ), "County should be 8 chars and start with G (e.g. 'G0100010')"
    assert len(args.PUMA) == 9 and args.PUMA[0] == "G", "PUMA should be 9 chars and start with G (e.g. 'G01002100')"

    sample_sizes = [int(i) for i in args.n_samples.split(",")]
    s = SampleOnly(args.buildstock_dir, args.output_dir)
    for i in sample_sizes:
        print(f"Creating {i} samples...")
        s.run_sampler(args.county, args.PUMA, i)


if __name__ == "__main__":
    main()
