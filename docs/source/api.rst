AA Toolbox API
==============

Configuration
-------------

.. autoclass:: ochanticipy.CountryConfig

.. autofunction:: ochanticipy.create_country_config
.. autofunction:: ochanticipy.create_custom_country_config

Data sources
------------

CHIRPS
^^^^^^

.. automodule:: ochanticipy.datasources.chirps.chirps

Daily
"""""

.. autoclass:: ochanticipy.ChirpsDaily
   :inherited-members:

Monthly
"""""""

.. autoclass:: ochanticipy.ChirpsMonthly
   :inherited-members:

Common Operational Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: ochanticipy.datasources.codab.codab
.. autoclass:: ochanticipy.CodAB
   :members:

FEWS NET
^^^^^^^^

.. automodule:: ochanticipy.datasources.fewsnet.fewsnet
.. autoclass:: ochanticipy.FewsNet
   :members:

GloFAS
^^^^^^

.. automodule:: ochanticipy.datasources.glofas.glofas

Reanalysis
""""""""""

.. autoclass:: ochanticipy.GlofasReanalysis
   :inherited-members:

Forecast
""""""""

.. autoclass:: ochanticipy.GlofasForecast
   :inherited-members:

Reforecast
""""""""""

.. autoclass:: ochanticipy.GlofasReforecast
   :inherited-members:

IRI
^^^

.. automodule:: ochanticipy.datasources.iri.iri_seasonal_forecast

Probability forecast
""""""""""""""""""""

.. autoclass:: ochanticipy.IriForecastProb
   :inherited-members:

Dominant forecast
"""""""""""""""""

.. autoclass:: ochanticipy.IriForecastDominant
   :inherited-members:

NDVI (USGS eMODIS)
^^^^^^^^^^^^^^^^^^

.. automodule:: ochanticipy.datasources.usgs.ndvi_products

Smoothed
""""""""

.. autoclass:: ochanticipy.UsgsNdviSmoothed
   :inherited-members:

Percent of median
"""""""""""""""""

.. autoclass:: ochanticipy.UsgsNdviPctMedian
   :inherited-members:

Median anomaly
""""""""""""""

.. autoclass:: ochanticipy.UsgsNdviMedianAnomaly
   :inherited-members:

Difference from previous year
"""""""""""""""""""""""""""""

.. autoclass:: ochanticipy.UsgsNdviYearDifference
   :inherited-members:

Utilities
---------

GeoboundingBox
^^^^^^^^^^^^^^

.. automodule:: ochanticipy.utils.geoboundingbox
.. autoclass:: ochanticipy.GeoBoundingBox
   :members:

Raster module
^^^^^^^^^^^^^

.. automodule:: ochanticipy.utils.raster

Data arrays
"""""""""""

.. autoclass:: ochanticipy.utils.raster.AatRasterArray
   :members:
   :inherited-members: RasterArray

Datasets
""""""""

.. autoclass:: ochanticipy.utils.raster.AatRasterDataset
   :members:
   :inherited-members: RasterDataset
