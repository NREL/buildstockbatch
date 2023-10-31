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
