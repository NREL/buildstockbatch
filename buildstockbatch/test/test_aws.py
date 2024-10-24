import os
import yaml
import logging

from buildstockbatch.aws.aws import AwsBatch

here = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level="DEBUG")  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)


def test_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file(
        update_args={
            "os_version": "3.8.0",
            "baseline": {
                "custom_gems": True,
                "n_buildings_represented": 80000000,
            },
            "aws": {
                "job_identifier": "testaws",
                "s3": {"bucket": "resbldg-datasets", "prefix": "testing/external_demo_project"},
                "region": "us-west-2",
                "use_spot": True,
                "batch_array_size": 100,
                "notifications_email": "user@example.com",
            },
        }
    )

    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)

    buildstock_directory = cfg["buildstock_directory"]

    batch = AwsBatch(project_filename)
    batch.build_image("aws")

    gem_list_log_log_path = os.path.join(
        buildstock_directory,
        "resources",
        ".cloud_docker_image",
        "openstudio_gem_list_output.log",
    )
    assert os.path.exists(gem_list_log_log_path)
    with open(gem_list_log_log_path, "r") as gem_list:
        contents = gem_list.read()
        custom_gem = "/var/oscli/gems/ruby/2.7.0/gems/openstudio-standards-0.2.0"
        assert custom_gem in contents


def test_no_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file(
        update_args={
            "os_version": "3.8.0",
            "aws": {
                "job_identifier": "testaws",
                "s3": {"bucket": "resbldg-datasets", "prefix": "testing/external_demo_project"},
                "region": "us-west-2",
                "use_spot": True,
                "batch_array_size": 100,
                "notifications_email": "user@example.com",
            },
        }
    )

    # Add aws to the project file
    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)

    buildstock_directory = cfg["buildstock_directory"]

    batch = AwsBatch(project_filename)
    batch.build_image("aws")

    gem_list_log_log_path = os.path.join(
        buildstock_directory,
        "resources",
        ".cloud_docker_image",
        "openstudio_gem_list_output.log",
    )
    assert os.path.exists(gem_list_log_log_path)
    with open(gem_list_log_log_path, "r") as gem_list:
        contents = gem_list.read()
        custom_gem = "/var/oscli/gems/ruby/2.7.0/gems/openstudio-standards-0.2.0"
        assert custom_gem not in contents
