CHIRPS
===

Background
----------

The Climate Hazards Center at UC Santa Barbara (`CHC <https://www.chc.ucsb.edu/>`_),
in collaboration with scientists at the USGS Earth Resources Observation and Science
(`EROS <https://www.usgs.gov/centers/eros>`_) Center, created the Climate Hazards group
Infrared Precipitation with Stations `CHIRPS <https://www.chc.ucsb.edu/data/chirps>`_
environmental record.

CHIRPS is a quasi-global (50&deg;S-50°N), high resolution (0.05°), daily, pentadal,
and monthly precipitation dataset, ranging from 1981 to near-present. It incorporates
CHPclim (CHC in-house climatology), 0.05° resolution satellite imagery, and in-situ station
data to create gridded rainfall time series for trend analysis and seasonal drought
monitoring. More information are available here: `<https://www.nature.com/articles/sdata201566>`.

Through the AA toolbox, it is possible to have access to daily and monthly data.

The data is downloaded from `IRI's maproom
<http://iridl.ldeo.columbia.edu/SOURCES/.UCSB/.CHIRPS/.v2p0>`_ as the platform allows
for the selection of geographical areas.


Usage
-----

In order to access CHIRPS data, you first need to create a country
configuration for the country you would like to get data for. In this example
we will use Burkina Faso, which corresponds to the ISO3 code "bfa":

.. code-block:: python

    from aatoolbox import create_country_config
    country_config = create_country_config(iso3="bfa")

Another required input is the geographic area of interest. A simple
way to identify the area around the chosen country is to use the COD
administrative boundaries. As a first step, you need to download them:

.. code-block:: python

    from aatoolbox import CodAB
    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load(admin_level=0)

Next, an instance of the class GeoBoundingBox needs to be created:

.. code-block:: python

    from aatoolbox import GeoBoundingBox
    geo_bounding_box = GeoBoundingBox.from_shape(admin0)

Finally you can make use of the Chirps classes: two classes are
available, depending on the time resolution of interest. Daily
data are available with two spatial resolutions (0.05 and 0.25 degrees),
whereas monthly data can be obtained only with a 0.05-degree resolution.

Moreover, when downloading the data, you can choose start and end date
of the dataset to be downloaded, by specifying start and end year, month,
and day. All parameters are optional: if not specified, the start
and end year will be respectively set to 1981 and the most recent year
for which the data is available on the server. Similar conventions
apply to start and end month and day. In the following example, monthly
data ranging from February 2001 to March 2006 is downloaded.

.. code-block:: python

    from aatoolbox import ChirpsMonthly

    chirps_monthly = ChirpsMonthly(country_config=country_config,
                                   geo_bounding_box=geo_bounding_box)

    chirps_monthly.download(start_year=2001, end_year=2006, start_month=2, end_month=3)

Similarly, the code snippet below allows to download daily CHIRPS data with 0.25 degrees
resolution and ranging from October 23, 2007 to the latest available data point:

.. code-block:: python

    from aatoolbox import ChirpsDaily

    chirps_daily = ChirpsDaily(country_config=country_config,
                               geo_bounding_box=geo_bounding_box)

    chirps_daily.download(start_year=2007, start_month=10, start_day=23)

After having downloaded the datasets, a processing step is needed before being able to use them.

.. code-block:: python

    chirps_monthly.process()
    chirps_daily.process()

Finally, the data can be loaded as an ``xarray`` dataset, which is the result of the merging of
all processed datasets, with fixed time resolution and location. When calling the ``load()``
method, it is possible to specify start and end date of the data of interest, expressed once
again as start (end) year, month and day (the latter only valid in case of daily data). If
no arguments are passed to the method, the loaded dataset will be constituted of all processed
datasets. If only certain arguments are passed (such as start year and end year), the others will
be automatically assigned following the conventions explained above.

Below are two examples of the use of the ``load`` method, respectively for daily and monthly data.

.. code-block:: python

    start_year = 2021
    end_year = 2021
    start_month = 5
    end_month = 9
    start_day = 30
    end_day = 5

    chirps_monthly_data = chirps_monthly.load()
    chirps_daily_data = chirps_daily.load(
        start_year=start_year,
        end_year=end_year,
        start_month=start_month,
        end_month=end_month,
        start_day=start_day,
        end_day=end_day
        )
