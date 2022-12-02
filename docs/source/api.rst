AA Toolbox API
==============

Configuration
-------------

.. autoclass:: aatoolbox.CountryConfig

.. autofunction:: aatoolbox.create_country_config
.. autofunction:: aatoolbox.create_custom_country_config

Data sources
------------

CHIRPS
^^^^^^

.. automodule:: aatoolbox.datasources.chirps.chirps

Daily
"""""

.. autoclass:: aatoolbox.ChirpsDaily
   :inherited-members:

Monthly
"""""""

.. autoclass:: aatoolbox.ChirpsMonthly
   :inherited-members:

Common Operational Datasets
^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: aatoolbox.datasources.codab.codab
.. autoclass:: aatoolbox.CodAB
   :members:

FEWS NET
^^^^^^^^

.. automodule:: aatoolbox.datasources.fewsnet.fewsnet
.. autoclass:: aatoolbox.FewsNet
   :members:

GloFAS
^^^^^^

.. automodule:: aatoolbox.datasources.glofas.glofas

Reanalysis
""""""""""

.. autoclass:: aatoolbox.GlofasReanalysis
   :inherited-members:

Forecast
""""""""

.. autoclass:: aatoolbox.GlofasForecast
   :inherited-members:

Reforecast
""""""""""

.. autoclass:: aatoolbox.GlofasReforecast
   :inherited-members:

IRI
^^^

.. automodule:: aatoolbox.datasources.iri.iri_seasonal_forecast

Probability forecast
""""""""""""""""""""

.. autoclass:: aatoolbox.IriForecastProb
   :inherited-members:

Dominant forecast
"""""""""""""""""

.. autoclass:: aatoolbox.IriForecastDominant
   :inherited-members:

NDVI (USGS eMODIS)
^^^^^^^^^^^^^^^^^^

.. automodule:: aatoolbox.datasources.usgs.ndvi_products

Smoothed
""""""""

.. autoclass:: aatoolbox.UsgsNdviSmoothed
   :inherited-members:

Percent of median
"""""""""""""""""

.. autoclass:: aatoolbox.UsgsNdviPctMedian
   :inherited-members:

Median anomaly
""""""""""""""

.. autoclass:: aatoolbox.UsgsNdviMedianAnomaly
   :inherited-members:

Difference from previous year
"""""""""""""""""""""""""""""

.. autoclass:: aatoolbox.UsgsNdviYearDifference
   :inherited-members:

Utilities
---------

GeoboundingBox
^^^^^^^^^^^^^^

.. automodule:: aatoolbox.utils.geoboundingbox
.. autoclass:: aatoolbox.GeoBoundingBox
   :members:

Raster module
^^^^^^^^^^^^^

.. automodule:: aatoolbox.utils.raster

Data arrays
"""""""""""

.. autoclass:: aatoolbox.utils.raster.AatRasterArray
   :members:
   :inherited-members: RasterArray

Datasets
""""""""

.. autoclass:: aatoolbox.utils.raster.AatRasterDataset
   :members:
   :inherited-members: RasterDataset
