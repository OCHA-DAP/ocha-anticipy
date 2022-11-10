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

However, since rioxarray already extends xarray, this
modules extensions inherit from the RasterArray and
RasterDataset extensions respectively. This ensures
cleaner code in the module as ``rio`` methods are
available immediately, but also means a couple of
design decisions are followed.

The ``xarray.DataArray`` and ``xarray.Dataset``
extensions here inherit from rioxarray base classes.
Thus, methods that are identical for both objects
are defined in a mixin class ``AatRasterMixin`` which
can be inherited by the two respective extensions.

Usage
-----

To make the extension accessible, you only need to import
``aatoolbox`` directly. As a simple example, we can create
a simple data array that has incorrect coordinates
(ascending latitude and descending longitude). Then
we can use the extension to invert them to
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
    da_inv.get_index("lat")

As above, for any data array (or
dataset) ``da``, the extension is available for use
directly using ``da.aat.method_or_property``.
