# -*- coding: utf-8 -*-

from .commercial.latest.commercial import CommercialDefaultWorkflowGenerator as latestComRenerator  # noqa F041
from .residential.latest.residential_hpxml import ResidentialHpxmlWorkflowGenerator as latestResGenerator  # noqa F041
from .residential import latest as residential_latest  # noqa F401
from .commercial import latest as commercial_latest  # noqa F401

version_map = {
    "commercial_default": {"latest": latestComRenerator, commercial_latest.__version__: latestComRenerator},
    "residential_hpxml": {"latest": latestResGenerator, residential_latest.__version__: latestResGenerator},
}
