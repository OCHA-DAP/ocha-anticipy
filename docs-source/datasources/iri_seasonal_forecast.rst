IRI Seasonal Forecast
=====================

Background
----------

The International Research Institute for Climate and Society (IRI) produces
`seasonal climate forecasts <https://iri.columbia
.edu/our-expertise/climate/forecasts/seasonal-climate-forecasts/>`_.

These forecasts predict measures of 3-monthly precipitation and temperature
with leadtimes up to 4 months. They produce two types of forecasts, the
flexible and tercile forecast. More information on their methodology can be
found `here <https://iri.columbia
.edu/our-expertise/climate/forecasts/seasonal-climate-forecasts/methodology
/>`_.

Currently in OCHA AnticiPy, only the tercile precipitation forecast has been
implemented.

The tercile precipitation forecast is published in two formats. One only
indicates the dominant tercile probability while the other format indicates
the probability for each tercile. Both formats are implemented in
OCHA AnticiPy.


The data is downloaded from `IRI's maproom
<https://iridl.ldeo.columbia.edu/maproom/Global/Forecasts/NMME_Seasonal_Forecasts/Precipitation_ELR.html>`_

Usage
-----

To download data from the IRI API, a key is required for
authentication, and must be set in the ``IRI_AUTH`` environment
variable. To obtain this key config you need to create an account
`here <https://iridl.ldeo.columbia.edu/auth/login>`_.
Note that this key might be invalid after some time, after which you have
to generate a new key `here <https://iridl.ldeo.columbia.edu/auth/genkey>`_.

To use the IRI class, you first need to create a country
configuration
for the country you would like to get data for. For this example we will use
Burkina Faso, which has the ISO3 "bfa":

.. code-block:: python

    from ochanticipy import create_country_config
    country_config = create_country_config(iso3="bfa")

The IRI class also requires a geographic area as input. A simple
way to get the area around a country of interest is to use the COD
administrative boundaries. The first step is to download them:

.. code-block:: python

    from ochanticipy import CodAB
    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load(admin_level=0)

Next, create a GeoBoundingBox for input to IRI:

.. code-block:: python

    from ochanticipy import GeoBoundingBox
    geo_bounding_box = GeoBoundingBox.from_shape(admin0)

Now we're ready to get the IRI data:

.. code-block:: python

    from ochanticipy import IriForecastDominant

    iri_dominant = IriForecastDominant(country_config=country_config,
                                       geo_bounding_box=geo_bounding_box)
    iri_dominant.download()
    iri_dominant.process()
    iri_dominant_data = iri_dominant.load()

We can take similar steps to get the data that indicates the probability per
tercile:

.. code-block:: python

    from ochanticipy import IriForecastProb

    iri_prob = IriForecastProb(country_config=country_config,
                               geo_bounding_box=geo_bounding_box)
    iri_prob.download()
    iri_prob.process()
    iri_prob_data = iri_prob.load()

The full code snippet is below in case you would like to copy it:

.. code-block:: python

    from ochanticipy import create_country_config, CodAB, GeoBoundingBox, \
                          IriForecastDominant, IriForecastProb

    country_config = create_country_config(iso3="bfa")

    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load(admin_level=0)

    geo_bounding_box = GeoBoundingBox.from_shape(admin0)


    iri_dominant = IriForecastDominant(country_config=country_config,
                                       geo_bounding_box=geo_bounding_box)
    iri_dominant.download()
    iri_dominant.process()
    iri_dominant_data = iri_dominant.load()


    iri_prob = IriForecastProb(country_config=country_config,
                               geo_bounding_box=geo_bounding_box)
    iri_prob.download()
    iri_prob.process()
    iri_prob_data = iri_prob.load()
