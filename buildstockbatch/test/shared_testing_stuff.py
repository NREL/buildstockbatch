import os
import pathlib
import pytest


resstock_directory = pathlib.Path(
    os.environ.get("RESSTOCK_DIR", pathlib.Path(__file__).resolve().parent.parent.parent.parent / "resstock")
)
resstock_required = pytest.mark.skipif(
    not resstock_directory.exists(),
    reason="ResStock checkout is not found"
)

comstock_directory = pathlib.Path(
    os.environ.get("COMSTOCK_DIR", pathlib.Path(__file__).resolve().parent.parent.parent.parent / "comstock")
)
comstock_required = pytest.mark.skipif(
    not comstock_directory.exists(),
    reason="ComStock checkout is not found"
)
