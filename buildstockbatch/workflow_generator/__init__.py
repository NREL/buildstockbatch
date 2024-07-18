# -*- coding: utf-8 -*-

from .commercial.latest.commercial import CommercialDefaultWorkflowGenerator as latestComRenerator  # noqa F041
from .residential.latest.residential_hpxml import ResidentialHpxmlWorkflowGenerator as latestResGenerator  # noqa F041
from .residential import latest as residential_latest  # noqa F401
from .commercial import latest as commercial_latest  # noqa F401

version2GeneratorClass = {
    "commercial_default": {"latest": latestComRenerator, commercial_latest.__version__: latestComRenerator},
    "residential_hpxml": {"latest": latestResGenerator, residential_latest.__version__: latestResGenerator},
}
version2info = {
    "commercial_default": {
        "latest": commercial_latest.version_info,
        commercial_latest.__version__: commercial_latest.version_info,
    },
    "residential_hpxml": {
        "latest": residential_latest.version_info,
        residential_latest.__version__: residential_latest.version_info,
    },
}
