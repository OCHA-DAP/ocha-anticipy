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

Accessor
^^^^^^^^

To make the extension accessible, you only need to import
``ochanticipy`` directly. Then, extension methods and properties
are simply accessed using ``da.oap.method()`` or
``da.oap.property`` on a data array or dataset.  As a simple
example, we can create a data array that has
incorrect coordinates (ascending latitude and descending
longitude). Then we can use the extension to invert them to
the correct ordering.

.. code-block:: python

    import xarray
    import numpy

    import ochanticipy

    da = xarray.DataArray(
      numpy.arange(16).reshape(4,4),
      coords={"lat":numpy.array([87, 88, 89, 90]),
              "lon":numpy.array([70, 69, 68, 67])}
    )
    da_inv = da.oap.invert_coordinates()

    # check they have inverted
    da_inv.get_index("lon")
    #> Int64Index([67, 68, 69, 70], dtype='int64', name='lon')
    da_inv.get_index("lat")
    #> Int64Index([90, 89, 88, 87], dtype='int64', name='lat')

Raster statistics
^^^^^^^^^^^^^^^^^

One of the more useful functionalities of the module
is providing an easy ability to calcuate raster statistics
across a specified set of geometries defined in a geodataframe.
Paired with the administrative boundaries and raster data sources
available through this library, we can easily calculate
raster statistics.

A full snippet of example code is available below.

.. code-block:: python

    import ochanticipy
    import datetime

    # load the administrative boundaries
    country_config = ochanticipy.create_country_config(iso3="eth")
    codab = ochanticipy.CodAB(country_config)
    codab.download()
    codab_eth = codab.load(admin_level=2)

    # get geobounding box for CHIRPS downloading
    geo_bounding_box = ochanticipy.GeoBoundingBox.from_shape(codab_eth)

    # load CHIRPs data for processing
    start_date = datetime.date(year=2001, month=2, day=1)
    end_date = datetime.date(year=2006, month=3, day=31)

    chirps_monthly = ChirpsMonthly(
        country_config=country_config,
        geo_bounding_box=geo_bounding_box,
        start_date=start_date,
        end_date=end_date
        )

    chirps_monthly.download()
    chirps_monthly.process()
    chirps_monthly_data = chirps_monthly.load()

    # compute raster statistics

    chirps_monthly_data.oap.compute_raster_stats(
      gdf=codab_eth,
      feature_col="ADM2_PCODE"
    )

Properties
----------

The user should be careful when accessing attributes of
a data array when using the raster module. This module
builds on `rioxarray <https://corteva.github.io/rioxarray>_`
extensions, and thus methods and attributes accessible
via ``da.rio.method()``  or ``da.rio.property`` are
also accessible using ``da.oap.method()`` or
``da.oap.property``. However, original rioxarray properties
should be accessed using ``da.rio.property``.

Let's create a simple data array where we want to specify
the spatial dimensions explicitly because the coordinate
names are not automatically detected.

.. code-block:: python

    import xarray
    import numpy
    import ochanticipy

    da = xarray.DataArray(
        numpy.arange(16).reshape(4,4),
        coords={"a":numpy.array([90, 89, 88, 87]),
                "b":numpy.array([70, 69, 68, 67])}
    )

We can set the spatial dimensions using
``da.rio.set_spatial_dims()`` or call it directly
from ``da.oap``.

.. code-block:: python

  da_new = da.oap.set_spatial_dims(
    x_dim="a",
    y_dim="b"
  )

However, even though we can set the dimensions
using either accessor, we have to be careful
accessing the properties.

.. code-block:: python

  da_new.rio.x_dim
  #> 'a'

  da_new.oap.x_dim
  #> MissingSpatialDimensionError: x dimension not found.
  #> 'rio.set_spatial_dims()' or using 'rename()' to change
  #> the dimension name to 'x' can address this.

Even though the method was called using ``oap``, the property
is not accessible through it. Users need to be careful about
accessing rioxarray properties using the ``oap`` accessor.

For best practice, rioxarray methods and properties should all
be accessed using ``rio``. These properties are ``rio.x_dim``,
``rio.y_dim``, ``rio.shape``, ``rio.width``, ``rio.height``, and
``rio.crs``. This module's methods and properties should be
accessed using the ``oap`` accessor. These properties are
``oap.t_dim`` and ``oap.longitude_range``.
