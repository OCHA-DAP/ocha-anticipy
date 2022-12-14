"""OCHA AnticiPy: Get the data you need for anticipating humanitarian risk.

The OCHA AnticiPy library contains modules designed to simplify the
downloading and processing of data related to the anticipation of
humanitarian risk, such as climate and food security data.
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
