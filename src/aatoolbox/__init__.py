"""aa-toolbox: Toolbox for Anticipatory Action.

The ``aatoolbox`` library contains modules designed to assist with
downloading and processing the data required for disaster-related
anticipatory action.
"""
from aatoolbox.config.countryconfig import create_country_config
from aatoolbox.datasources.codab.codab import CodAB
from aatoolbox.datasources.iri.iri_seasonal_forecast import (
    IriForecastDominant,
    IriForecastProb,
)
from aatoolbox.datasources.usgs.usgs_ndvi import (
    UsgsNdviMedianAnomaly,
    UsgsNdviPctMedian,
    UsgsNdviSmoothed,
    UsgsNdviYearDifference,
)
from aatoolbox.utils.geoboundingbox import GeoBoundingBox

from ._version import version as __version__

__all__ = (
    "create_country_config",
    "CodAB",
    "GeoBoundingBox",
    "IriForecastProb",
    "IriForecastDominant",
    "UsgsNdviSmoothed",
    "UsgsNdviPctMedian",
    "UsgsNdviMedianAnomaly",
    "UsgsNdviYearDifference",
    "__version__",
)
