Data Sources
============

OCHA AnticiPy supports the following data sources:

.. toctree::
   :maxdepth: 1

   datasources/chirps
   datasources/codab
   datasources/fewsnet
   datasources/glofas
   datasources/iri_seasonal_forecast
   datasources/usgs_ndvi

In general, each data source is available as a class with three methods:

``download``: Download the data

``process``: Minimally process the data to a more usable format

``load``: Return the processed data as an object to work with in Python
