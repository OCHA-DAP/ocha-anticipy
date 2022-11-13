Raster
======

Background
----------

This module extends xarray and rioxarray to provide
additional functionality for raster processing and
post-processing. These are used throughout the
datasources to process various raster data, but are
also useful directly for the end user, and is thus
available for direct use. The extension is based on the
guidance for how to `extend xarray
<http://xarray.pydata.org/en/stable/internals/extending-xarray.html>`_.

Usage
-----

To make the extension accessible, you only need to import
``aatoolbox`` directly. Then, extension methods and properties
are simply accessed using ``da.aat.method()`` or
``da.aat.property`` on a data array or dataset.  As a simple
example, we can create a data array that has
incorrect coordinates (ascending latitude and descending
longitude). Then we can use the extension to invert them to
the correct ordering.

.. code-block:: python

    import xarray
    import numpy
    import aatoolbox

    da = xarray.DataArray(
      numpy.arange(16).reshape(4,4),
      coords={"lat":numpy.array([87, 88, 89, 90]),
              "lon":numpy.array([70, 69, 68, 67])}
    )
    da_inv = da.aat.invert_coordinates()

    # check they have inverted
    da_inv.get_index("lon")
    #> Int64Index([67, 68, 69, 70], dtype='int64', name='lon')
    da_inv.get_index("lat")
    #> Int64Index([90, 89, 88, 87], dtype='int64', name='lat')

One of the more useful functionalities of the module
is providing an easy ability to calcuate raster statistics
across a specified set of geometries defined in a geodataframe.
A full snippet of example code is available below.

.. code-block:: python

    import aatoolbox

    # load the administrative boundaries
    country_config = aatoolbox.create_country_config(iso3="eth")
    codab = aatoolbox.CodAB(country_config)
    codab.download()
    codab_eth = codab.load(admin_level=2)

    # load NDVi data for processing for one dekad
    ndvi_smooth = UsgsNdviSmoothed(
        country_config=country_config,
        start_date="2021-01-01",
        end_date="2021-01-01"
    )

    ndvi_smooth.download()
    ndvi_da = ndvi_smooth.load_raster(date=(2021, 1))
    ndvi_da.aat.compute_raster_stats(
      gdf=codab_eth,
      feature_col="ADM2_PCODE"
    )

This functionality is already incorporated into the NDVI module that
streamlines the raster statistics calculated across time as well.
However, the example above shows how the raster module can be applied
to any raster datasets or arrays you may have.
