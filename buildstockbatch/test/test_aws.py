import os
import yaml
import logging

from buildstockbatch.aws.aws import AwsBatch

here = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(level="DEBUG")  # Use DEBUG, INFO, or WARNING
logger = logging.getLogger(__name__)


def test_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()

    # Add aws and custom_gems to the project file
    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)
    # custom_gems
    cfg["baseline"]["custom_gems"] = True
    # AWS
    cfg["aws"] = {}
    cfg["aws"]["job_identifier"] = "testaws"
    cfg["aws"]["s3"] = {}
    cfg["aws"]["s3"]["bucket"] = "resbldg-datasets"
    cfg["aws"]["s3"]["prefix"] = "testing/external_demo_project"
    cfg["aws"]["region"] = "us-west-2"
    cfg["aws"]["use_spot"] = True
    cfg["aws"]["batch_array_size"] = 100
    cfg["aws"]["notifications_email"] = "user@example.com"
    with open(project_filename, "w") as f:
        yaml.dump(cfg, f)

    buildstock_directory = cfg["buildstock_directory"]

    batch = AwsBatch(project_filename)
    batch.build_image()

    gem_list_log_log_path = os.path.join(
        buildstock_directory,
        "resources",
        ".aws_docker_image",
        "openstudio_gem_list_output.log",
    )
    assert os.path.exists(gem_list_log_log_path)
    with open(gem_list_log_log_path, "r") as gem_list:
        contents = gem_list.read()
        custom_gem = "/var/oscli/gems/ruby/2.7.0/gems/openstudio-standards-0.2.0"
        assert custom_gem in contents


def test_no_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()

    # Add aws to the project file
    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)
    # AWS
    cfg["aws"] = {}
    cfg["aws"]["job_identifier"] = "testaws"
    cfg["aws"]["s3"] = {}
    cfg["aws"]["s3"]["bucket"] = "resbldg-datasets"
    cfg["aws"]["s3"]["prefix"] = "testing/external_demo_project"
    cfg["aws"]["region"] = "us-west-2"
    cfg["aws"]["use_spot"] = True
    cfg["aws"]["batch_array_size"] = 100
    cfg["aws"]["notifications_email"] = "user@example.com"
    with open(project_filename, "w") as f:
        yaml.dump(cfg, f)

    buildstock_directory = cfg["buildstock_directory"]

    batch = AwsBatch(project_filename)
    batch.build_image()

    gem_list_log_log_path = os.path.join(
        buildstock_directory,
        "resources",
        ".aws_docker_image",
        "openstudio_gem_list_output.log",
    )
    assert os.path.exists(gem_list_log_log_path)
    with open(gem_list_log_log_path, "r") as gem_list:
        contents = gem_list.read()
        custom_gem = "/var/oscli/gems/ruby/2.7.0/gems/openstudio-standards-0.2.0"
        assert custom_gem not in contents
