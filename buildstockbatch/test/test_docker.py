import os
import pytest
import yaml

from buildstockbatch.local import LocalBatch

here = os.path.dirname(os.path.abspath(__file__))


@pytest.mark.skip(reason="We need to change the way we're installing custom gems")
def test_custom_gem_install(basic_residential_project_file):
    project_filename, results_dir = basic_residential_project_file()

    # Add custom_gems to the project file
    with open(project_filename, "r") as f:
        cfg = yaml.safe_load(f)
    cfg["baseline"]["custom_gems"] = True
    with open(project_filename, "w") as f:
        yaml.dump(cfg, f)

    buildstock_directory = cfg["buildstock_directory"]

    LocalBatch(project_filename)

    bundle_install_log_path = os.path.join(
        buildstock_directory, "resources", ".custom_gems", "bundle_install_output.log"
    )
    assert os.path.exists(bundle_install_log_path)
    os.remove(bundle_install_log_path)

    gem_list_log_log_path = os.path.join(
        buildstock_directory,
        "resources",
        ".custom_gems",
        "openstudio_gem_list_output.log",
    )
    assert os.path.exists(gem_list_log_log_path)
    os.remove(gem_list_log_log_path)
