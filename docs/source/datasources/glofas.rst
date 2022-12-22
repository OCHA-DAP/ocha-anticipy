GloFAS
======

Background
----------

The
`Global Flood Awareness System (GloFAS)
<https://www.globalfloods.eu/>`_
is part of the
`Copernicus Emergency Management Service (CEMS).
<https://emergency.copernicus.eu/>`_.
It provides a global model and forecast of river flow
for large catchments and riverine flooding,
intended to be complementary to national hydrological and
meteorological services.

The processed and curated data,
plus several other products, are available to view through the GloFAS
`map viewer
<https://www.globalfloods.eu/glofas-forecasting/>`_.
While the map viewer makes image graphs of modelled and forecasted river
discharge available, the actual data are needed to perform more in-depth
analyses, such as computing specific return periods, knowing when
a certain river discharge threshold was exceeded in the past, or
detailed forecast performance at a specific location,

The raw data in raster format can be downloaded from  the
`Climate Data Store
<https://cds.climate.copernicus.eu/#!/home>`_.
However, the API can be tricky to navigate, with query size limits
and long download times which depend on the query structure.
Furthermore, the data are in the less commonly used GRIB
file format, and the river discharge data at specific
reporting points (those found on the map viewer)
still need to be extracted. This GloFAS Python module takes
care of all of these steps.

A note about model versions: On 26 May 2021 GloFAS released `version 3
<https://www.copernicus.eu/en/news/news/observer-whats-new-latest-glofas-31-release>`_
of their model. While version 2 is still available on CDS, it
will soon be phased out. Thus this module is currently only able
to download version 3.

Before describing the module usage, we will outline the different
GloFAS datasets available: reanalysis, forecast, and reforecast.

Reanalysis
~~~~~~~~~~

GloFAS-ERA5 reanalysis
`(Harrigan et al.)
<https://essd.copernicus.org/articles/12/2043/2020/>`_
is a global gridded dataset of river discharge with
a daily timestep and resolution of 0.1°,
available from 1 January 1979 to the present day (updated daily).
It is based on ECMWF's latest global atmospheric reanalysis
`(ERA5, Herbasch et al.)
<https://rmets.onlinelibrary.wiley.com/doi/10.1002/qj.3803>`_
combined with the
`Lisflood
<https://ec-jrc.github.io/lisflood/>`_
hydrological and channel routing model,
and calibrated against observed river discharge
`(Alfieri et al.)
<https://www.sciencedirect.com/science/article/pii/S2589915519300331>`_.
The data is available to download
`here on CDS
<https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-historical?tab=overview>`_.

Forecast
~~~~~~~~

The GloFAS forecast
`(Harrigan et al.)
<https://hess.copernicus.org/preprints/hess-2020-532/>`_
uses the above model, applied to the
`ECMWF integrated forecast system (IFS) ensemble forecast
<https://www.ecmwf.int/en/publications/ifs-documentation>`_.
The IFS has 51 ensemble members out to a lead time of 15 days.
The ensemble is composed of a single control member, generated from
the most accurate estimate of current conditions, and 50 other
members that have their initial conditions perturbed.

Twice per week (Mondays and Thursdays), the IFS is extended to run up to 46 days ahead
at a coarser resolution (~36 km). GloFAS makes available days
16 to 30 for this extended forecast.

Version 3 of the forecast data is available to download from 26 May 2021 until
present day (updated daily) `here
<https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-forecast?tab=overview>`_.
While CDS does have version 3 pre-release data from 2020-2021,
we understand that there were some small issues that were fixed
in the operational version, so at this point in time this module
only retrieves data from the operational model.

Reforecast
~~~~~~~~~~

The GloFAS reforecast
`(Harrigan et al.)
<https://hess.copernicus.org/preprints/hess-2020-532/>`_
is similar to the forecast, but uses the ECMWF IFS
`reforecast
<https://www.ecmwf.int/en/forecasts/documentation-and-support/extended-range/re-forecast-medium-and-extended-forecast-range>`_.
The reforecast is initialized twice per week (Monday and Thursdays)
and has 11 ensemble members.
The data runs from January 1999 to December 2018,
and is available to download
`from CDS here
<https://cds.climate.copernicus.eu/cdsapp#!/dataset/cems-glofas-reforecast?tab=overview>`_.

Usage
-----

This module performs three basic steps:

#. Download the raw GloFAS data, in GRIB file format.
   Each file is a raster based on a
   user-specified region of interest, and contains data for a full
   day, month, or year, depending on the GloFAS product.
#. Process the data. For each raw GRIB file,
   river discharge for a set of user-provided pixel locations is extracted
   and stored in NetCDF format.
#. Load the data. The processed files, which are split by time in the same
   way as the raw data,
   are read in as a single `xarray.DataSet`.

CDS API
~~~~~~~

The download step makes use of the
`CDS API
<https://cds.climate.copernicus.eu/api-how-to>`_.
You will need to register an account on CDS, then once logged in go to your
`user page
<https://cds.climate.copernicus.eu/user/>`_
and note down your UID and API key.

The
`how-to page
<https://cds.climate.copernicus.eu/api-how-to>`_
has instructions for each operating system, but we summarize the instructions below:

Windows: Create a file called `%USERPROFILE%\.cdsapirc` where, `%USERPROFILE%` is usually located
in your `C:\Users\Username` folder.

Mac and Linux: Create the file `~/.cdsapirc`.

Then, for all operating systems, add the following to the created file:

.. code-block:: shell

    url: https://cds.climate.copernicus.eu/api/v2
    key: ${UID}:${API-key}

where `$UID` is your UID and `$API-key` is your API key.

ecCodes
~~~~~~~

To read in .GRIB files using `xarray`, you'll need to install
`ecCodes
<https://confluence.ecmwf.int/display/ECC/What+is+ecCodes>`_
on your machine.
For all operating systems, this can be done through
the
`source distribution
<https://confluence.ecmwf.int/display/ECC/ecCodes+installation>`_,
or in a `conda` environment with the command:

.. code-block:: shell

    conda install -c conda-forge eccodes

Some OS-specific binaries are also available. For Linux,
`python3-eccodes` can be found as a `.deb` and `.rpm`
(check your specific Linux distribution for the latest version).
For Mac, according to the
`ecCodes-Python documentation
<https://github.com/ecmwf/eccodes-python#system-dependencies>_`,
ecCodes can be installed using `brew`:

.. code-block:: shell

    brew install eccodes

Reporting points
~~~~~~~~~~~~~~~~

Next, if it :ref:`doesn't already exist<list of supported countries>`,
you need to create a country configuration
for the country you would like to analyze.

An example country config for Bangladesh is:

.. code-block:: yaml

    iso3: bgd
    glofas:
      reporting_points:
      - name: Bahadurabad
        lon: 89.65
        lat: 25.15
      - name: Hardinge Bridge
        lon: 89.05
        lat: 24.05

The reporting points indicate the raster file coordinates used
to extract the river discharge for a particular location.
Those in the above example have been taken from the
`GloFAS map viewer
<https://www.globalfloods.eu/glofas-forecasting/>`_.
If you select "Reporting Points" from the "Hydrological"
menu at the top, they will appear as dots on the map. If you then
click on one of the points, you are able to see
information such as the station name, and LISFLOOD X and Y, which are
the respective longitude and latitude used in the configuration file.

Reporting point coordinates are manually selected by the GloFAS team to
be representative of physical gauge locations, and to be located on a river
in the model raster file. In principle, one could
specify any set of coordinates that exists on the raster, but caution is advised
when doing so.


Running the code
~~~~~~~~~~~~~~~~

You can initialize a built-in country config as follows:

.. code-block:: python

    from ochanticipy import create_country_config

    country_config = create_country_config(iso3="bgd")

Another required input is the geographic area of interest, which will
define the bounds of raw raster data to be downloaded. A simple
way to identify the area around the chosen country is to use the COD
administrative boundaries. You will need to download the data,
and extract a geo bounding box:

.. code-block:: python

    from ochanticipy import CodAB, GeoBoundingBox

    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load()
    geo_bounding_box = GeoBoundingBox.from_shape(admin0)

Note that the reporting points in the configuration file need to lie within
the geographic area of interest.

Next you need to instantiate the GloFAS class with the country config. For this
example, we will use the GloFAS forecast, however the steps are nearly identical for the
the reanalysis and reforecast -- the only differences are the acceptable date ranges,
and that `leadtime_max` is **not** an input parameter to the reanlaysis.

In this case, we would like the data for the past month. In general, we suggest
specifying dates explicitly, as using e.g. `date.today()` will not produce
the same results when run on a different day.

.. code-block:: python

    from datetime import date

    from ochanticipy import GlofasForecast

    glofas_forecast = GlofasForecast(
        country_config=country_config,
        geo_bounding_box=geo_bounding_box,
        leadtime_max=15,
        start_date=date(year=2022, month=9, day=22)
        end_date=date(year=2022, month=10, day=22),
     )

We then need to download the GloFAS data. The module will download all the data
between `start_date` and `end_date` (inclusive). The raw files are in .GRIB format,
and are separated by day (forecast), month (reforecast), or year (reanalysis)
depending on the data type, due to CDS query size limits.

.. code-block:: python

    glofas_forecast.download()

The downloading process works by generating all requests required for the
specified timeframe, sending them to CDS, and saving the request numbers in memory.
You can see all your requests on the
`CDS website
<https://cds.climate.copernicus.eu/cdsapp#!/yourrequests>`_,
and cancel any pending ones in case the run is interrupted (we hope to implement
request number caching in a later version).

The module then pings the CDS API every minute to check which requests have
completed, and downloads those that have. This continues until all requests
have been downloaded.

This process can unfortunately take a long time, however, with this module
we've tried to optimize the queries to be as fast as possible. In our experience,
downloading the full reanalysis (from 1979 to today) is the fastest and takes a
couple of hours, while the full reforecast (1999 to 2018) is the slowest and takes
around a day to complete. It also depends how busy the queue is, which
you can check
`here
<https://cds.climate.copernicus.eu/live/queue>`_.

The next step is to process the files. First, we want to convert from the
less used GRIB format to the more common and flexible NetCDF. Furthermore, rather
than having the full raster, we extract the river discharge data at the
reporting poitns specified in the configuration file. This can be done in a single
step:

.. code-block:: python

    glofas_forecast.process()

Note that each individual raw GRIB file is converted to a corresponding
processed NetCDF file. This is to simplify the downloading and processing of
addition data, i.e. for adding new dates.

To load all of the processed files into a single dataframe (which can then
be saved to a single NetCDF file or other compatible format), execute:

.. code-block:: python

    bgd_glofas_forecast_reporting_points = glofas_forecast.load()

The full codde snippet is below:

.. code-block:: python

    from datetime import date

    from ochanticipy import create_country_config, CodAB, \
        GeoBoundingBox, GlofasForecast

    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load()
    geo_bounding_box = GeoBoundingBox.from_shape(admin0)

    glofas_forecast = GlofasForecast(
        country_config=country_config,
        geo_bounding_box=geo_bounding_box,
        leadtime_max=15,
        start_date=date(year=2022, month=9, day=22)
        end_date=date(year=2022, month=10, day=22),
     )
    glofas_forecast.download()
    glofas_forecast.process()

    bgd_glofas_forecast_reporting_points = glofas_forecast.load()
