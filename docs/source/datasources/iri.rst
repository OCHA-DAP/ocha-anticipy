IRI
===

Background
----------

Data is downloaded from `IRI's maproom
<https://iridl.ldeo.columbia.edu/maproom/Global/Forecasts/NMME_Seasonal_Forecasts/Precipitation_ELR.html>`_

For now only the tercile precipitation forecast has been
implemented. This forecast is published in two formats,
namely the dominant tercile probability and the probability
per tercile. Both variations are implemented here.

Usage
-----

To download data from the IRI API, a key is required for
authentication, and must be set in the ``IRI_AUTH`` environment
variable. To obtain this key config you need to create an account
`here <https://iridl.ldeo.columbia.edu/auth/login>`_.
Note that this key might be changed over time, and need to be updated
regularly.

To use the class, you first need to create a country configuration
for the country you would like to get data for. For this example we will use
Burkina Faso, which has the ISO3 "bfa":

.. code-block:: python

    from aatoolbox import create_country_config
    country_config = create_country_config(iso3="bfa")

The IRI class also requires a geographic area as input. A simple
way to get the area around a country of interest is to use the COD
administrative boundaries. The first step is to download them:

.. code-block:: python

    from aatoolbox import CodAB
    codab = CodAB(country_config=country_config)
    codab.download()
    admin0 = codab.load(admin_level=0)

Next, create a GeoBoundingBox for input to IRI:

.. code-block:: python

    from aatoolbox import GeoBoundingBox
    geo_bounding_box = GeoBoundingBox.from_shape(admin0)

Now we're ready to get the IRI data:

.. code-block:: python

    from aatoolbox import IriForecastDominant

    iri_dominant = IriForecastDominant(country_config=country_config,
                                       geo_bounding_box=geo_bounding_box)
    iri_dominant.download()
    iri_dominant.process()
    iri_dominant_data = iri_dominant.load()

We can take similar steps for the tercile probability forecast:

.. code-block:: python

    from aatoolbox import IriForecastProb

    iri_prob = IriForecastProb(country_config=country_config,
                               geo_bounding_box=geo_bounding_box)
    iri_prob.download()
    iri_prob.process()
    iri_prob_data = iri_prob.load()

The full code snippet is below in case you would like to copy it:

.. code-block:: python

    from aatoolbox import create_country_config, CodAB, GeoBoundingBox, \
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
