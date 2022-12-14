"""aa-toolbox: Toolbox for Anticipatory Action.

The ``ochanticipy`` library contains modules designed to assist with
downloading and processing the data required for disaster-related
anticipatory action.
"""
from ochanticipy.config.countryconfig import (
    CountryConfig,
    create_country_config,
    create_custom_country_config,
)
from ochanticipy.datasources.chirps.chirps import ChirpsDaily, ChirpsMonthly
from ochanticipy.datasources.codab.codab import CodAB
from ochanticipy.datasources.fewsnet.fewsnet import FewsNet
from ochanticipy.datasources.glofas.forecast import (
    GlofasForecast,
    GlofasReforecast,
)
from ochanticipy.datasources.glofas.reanalysis import GlofasReanalysis
from ochanticipy.datasources.iri.iri_seasonal_forecast import (
    IriForecastDominant,
    IriForecastProb,
)
from ochanticipy.datasources.usgs.ndvi_products import (
    UsgsNdviMedianAnomaly,
    UsgsNdviPctMedian,
    UsgsNdviSmoothed,
    UsgsNdviYearDifference,
)
from ochanticipy.utils import raster
from ochanticipy.utils.geoboundingbox import GeoBoundingBox

from ._version import version as __version__

__all__ = (
    "CountryConfig",
    "create_country_config",
    "create_custom_country_config",
    "ChirpsDaily",
    "ChirpsMonthly",
    "CodAB",
    "FewsNet",
    "GlofasReanalysis",
    "GlofasForecast",
    "GlofasReforecast",
    "IriForecastProb",
    "IriForecastDominant",
    "UsgsNdviSmoothed",
    "UsgsNdviPctMedian",
    "UsgsNdviMedianAnomaly",
    "UsgsNdviYearDifference",
    "GeoBoundingBox",
    "raster",
    "__version__",
)
