NDVI (USGS eMODIS)
==================

Warning
-------

The MODIS sensor has been reported by USGS to
have degraded in quality since May 2022 (dekad 13), and
updates to this data source have stopped. Documentation of
the product has been removed from the
`USGS FEWS product page <https://earlywarning.usgs.gov/fews/product>`_,
which has made methodological details and geographic links unavailable.

This module remains for users to have access to historic data but
recent data is unavailable and care should be used
analyzing any data since dekad 13 of 2022.

Background
----------

The United States Geological Survey, `USGS <https://www.usgs.gov/>`_ provides a wide
range of geo-spatial data and satellite imagery products in support of the
Famine Early Warning Systems Network, `FEWS NET <https://earlywarning.usgs.gov/fews>`_.
These products are used to support FEWS NET drought monitoring efforts, from rainfall
data to vegetation measurements.

The availability of data for each source varies,
but are often available for every dekad or pentad for a number of years. The
geographic coverage is typically comprehensive of the African continent, where
the majority of FEWS NET's work is focused, with additional areas covered on an
ad hoc basis. This module for now just provides access to the normalized
difference vegetation index (`NDVI <https://en.wikipedia.org/wiki/Normalized_difference_vegetation_index>`_)
generated from eMODIS AQUA provided by the USGS.

NDVI data is generated from eMODIS AQUA and published data includes temporally smoothed NDVI, median anomaly,
difference from the previous year, and median anomaly presented as a percentile.
The data is made available for the following areas of coverage:

- north-africa
- east-africa
- southern-africa
- west-africa
- central-asia
- yemen
- central-america
- hispaniola

Data is published at the dekadal level, and is released soon after the end of the dekad.
After a period of 3 dekads, data is updated with temporal smoothing and error correction
for cloud cover. Files for a specific dekad and region can range from 30MB up to over 100MB,
so downloading and processing can take a long time.


USGS published its data through its web portal,
which allows downloading data for a single geographical area and dekad. However, the data
is no longer made available there, and the data for this product are extracted
from the `back end data explorer <https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/fews/web/africa/east/dekadal/emodis/ndvi_c6/temporallysmoothedndvi/downloads/monthly/>`_.
This module is designed to allow programmatic access to and analysis of NDVI data from the data explorer.

Usage
-----

To use this class, you first need to create a country configuration
for the country you would like to analyze. You also need to supply the area
of coverage that the country data is contained in. Although the NDVI data
are no longer available directly on the portal, you can derive the region
name for the data using the primary
`USGS FEWS data portal <https://earlywarning.usgs.gov/fews>`_.
The valid values of the area names are listed above under Background.

An example country config for Ethiopia is:

.. code-block:: python

    iso3: eth
    usgs_ndvi:
        region_name: east-africa

For a number of countries this config has been implemented already.
For others, you can create a custom country config.
Once you have created the config you can load it:

.. code-block:: python

    from ochanticipy import create_country_config
    country_config = create_country_config(iso3="eth")

Next you need to instantiate the USGS NDVI class with the country config. For this
example, we will use the class for accessing temporally smoothed NDVI values. Since
the size of the data we will be working with is so large, we will only look at a
sample of data from a few dekads, corresponding to the dates in January:

.. code-block:: python

    from ochanticipy import UsgsNdviSmoothed
    ndvi_smooth = UsgsNdviSmoothed(
        country_config=country_config,
        start_date="2021-01-01",
        end_date="2021-01-31"
    )

We will first need to download the NDVI data if you haven't used the class before.
The code downloads all data between `start_date` and `end_date`. Be careful if not
setting start and end date during instantiation, the method will automatically
look for data between the first available data in the 19th dekad of 2002 and today.
We simply have to call the method to download available data.

.. code-block:: python

    ndvi_smooth.download()

Since each NDVI file is downloaded as a .tif, we can load them in individually as
rasters. We can do this for a specific date. This time we will specify the date
as a year and dekad tuple, although specify as a `datetime.date` or string as
above is equally appropriate.

.. code-block:: python

    ndvi_2021_01 = ndvi_smooth.load_raster(date=(2021, 1))

If time series analysis is desired on the NDVI data, the user can manually do this
using loaded in data arrays. However, given the size of the individual raster files,
this module provides the `process()` method that calculate statistics for
a given area. For this, we need to provide a geodataframe and column to aggregate to.

Let's load the country administrative boundaries for Ethiopia from our country
config, and use this to calculate basic statistics for the dekads we've loaded.

.. code-block:: python

    from ochanticipy import CodAB

    eth_cod = CodAB(country_config=country_config)
    # assuming you've downloaded the file already
    eth_gdf0 = eth_cod.load(admin_level=0)
    ndvi_smooth.process(
        gdf=eth_gdf0,
        feature_col="ADM0_EN"
    )

We can then load in and use the calculated statistics using `load()`. Since the
processed file is saved based on the `feature_col` name, we have to pass
`feature_col` to load in the correct data.

.. code-block:: python

    ndvi_smooth.load(feature_col="ADM0_EN")
