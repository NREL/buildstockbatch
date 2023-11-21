import docker
from docker.errors import DockerException
import os
import pathlib
import pytest


resstock_directory = pathlib.Path(
    os.environ.get(
        "RESSTOCK_DIR",
        pathlib.Path(__file__).resolve().parent.parent.parent.parent / "resstock",
    )
)
resstock_required = pytest.mark.skipif(not resstock_directory.exists(), reason="ResStock checkout is not found")


def check_docker_available():
    try:
        docker.from_env()
    except DockerException:
        return False
    else:
        return True


docker_available = pytest.mark.skipif(not check_docker_available(), reason="Docker isn't running on this machine")
