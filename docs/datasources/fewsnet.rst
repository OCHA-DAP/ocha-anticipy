FEWS NET
========

Background
----------

`FEWS NET <https://fews.net>`_, the Famine Early Warning Systems Network,
provides analyses on acute food insecurity.

FEWS NET uses the `IPC scale <https://fews.net/IPC>`_, which consists of 5
classes, where 1 indicates minimal food insecurity and 5 famine.

FEWS NET has been publishing data since 2009 for a selection of countries.

The data contains figures for the Current Situation, Near-Term projections, and
Medium term projections. This data is published 3 times a year, where each
projection period has a validity of 4 months. Till 2016 data was published
4 times a year, where the projection periods had a validity of 3 months.
In some exceptional cases where the situation has significantly changed from the projected situation, additional updates are published.

FEWS NET `publishes its data <https://fews.net/fews-data/333>`_ as maps on their website and this same data can be downloaded in the format of a shapefile. The maps either cover a region, e.g. East Africa, or a country. Each country is divided into livelihood areas, and to each livelihood area an IPC phase is assigned. This assigned phase is the highest phase at which at least 20% of the region’s population is classified.
The only data provided is thus one phase per livelihood zone, i.e the distribution of the population per phase is not provided. For some livelihood areas there might be no data available, which is indicated by a value of 99.

Besides this machine readable data, FEWS NET publishes elaborate reports that
explain the context of the data. These reports can also be found on their
website.


Usage
-----

To use this class, you first need to create a country configuration
for the country you would like to use:

.. code-block:: python

    from ochanticipy import create_country_config
    country_config = create_country_config(iso3="eth")

Next you need to instantiate the FEWS NET class with the country config:

.. code-block:: python

    from ochanticipy import FewsNet
    fewsnet = FewsNet(country_config=country_config)

Upon first use, you will need to downlaod the FEWS NET data.
The code downloads one date per call. You thus need to input the
publication year and month of your data of interest. This normally refers
to the start month of the *Current Situation period*, which is indicated
when you browse the data on the FEWS NET website.
For some dates only regional data is available, other dates have availability of country files.
The code automatically downloads the country file if available and else download the file covering the region the country is located in.

An example of the use of the download function:

.. code-block:: python

    fewsnet.download(pub_year=2021, pub_month=6)

Next, use the load method to begin working with the data as a
GeoPandas dataframe. The code loads one projection period per call.
The valid inputs of the projeciton period are
``CS`` (Current Situation),
``ML1`` (Near Term projection), and
``ML2`` (Medium Term projection).
An example of the load function thus is :

.. code-block:: python

    gdf_202106_ml1 = fewsnet.load(pub_year=2021, pub_month=6, projection_period="ML1")

The full code snippet is below in case you would like to copy it:

.. code-block:: python

    from ochanticipy import create_country_config, FewsNet
    country_config = create_country_config(iso3="eth")
    fewsnet = FewsNet(country_config=country_config)
    fewsnet.download(pub_year=2021, pub_month=6)
    gdf_202106_ml1 = fewsnet.load(pub_year=2021, pub_month=6, projection_period="ML1")

Configuration
-------------

The FEWS NET portion of the configuration file
should be setup as follows:

.. code-block:: yaml

    fewsnet:
        region_name: east-africa

Makes sure FEWS NET covers the country
of interest. You can see which countries are covered on `their website <https://
fews.net>`_ by clicking on *COUNTRIES & REGIONS*. Here you can also see which
region the country belongs to. This information needs to be added to the
country config.
The valid values of the region name are
``caribbean-central-america``,
``central-asia``,
``east-africa``,
``southern-africa``, and
``west-africa``.
