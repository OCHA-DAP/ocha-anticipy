"""aa-toolbox: Toolbox for Anticipatory Action.

The ``aatoolbox`` library contains modules designed to assist with
downloading and processing the data required for disaster-related
anticipatory action.
"""
from aatoolbox.config.countryconfig import (
    create_country_config,
    create_custom_country_config,
)
from aatoolbox.datasources.chirps.chirps import ChirpsDaily, ChirpsMonthly
from aatoolbox.datasources.codab.codab import CodAB
from aatoolbox.datasources.fewsnet.fewsnet import FewsNet
from aatoolbox.datasources.iri.iri_seasonal_forecast import (
    IriForecastDominant,
    IriForecastProb,
)
from aatoolbox.datasources.usgs.ndvi_products import (
    UsgsNdviMedianAnomaly,
    UsgsNdviPctMedian,
    UsgsNdviSmoothed,
    UsgsNdviYearDifference,
)
from aatoolbox.utils import raster
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

from ._version import version as __version__

__all__ = (
    "raster",
    "create_country_config",
    "create_custom_country_config",
    "ChirpsDaily",
    "ChirpsMonthly",
    "CodAB",
    "GeoBoundingBox",
    "IriForecastProb",
    "IriForecastDominant",
    "FewsNet",
    "UsgsNdviSmoothed",
    "UsgsNdviPctMedian",
    "UsgsNdviMedianAnomaly",
    "UsgsNdviYearDifference",
    "__version__",
)
