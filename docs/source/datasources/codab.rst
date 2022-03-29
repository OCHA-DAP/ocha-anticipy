COD AB
======

Background
----------

`Common Operational Datasets <https://cod.unocha.org>`_
(CODs) are definitive reference datasets governed by OCHA Field Information
Section (FIS) and designed
to support decision making during a humanitarian response.
The Administrative Boundary (AB) CODs are geospatial datasets that
delineate a country's borders and internal regions.
A key feature of the COD AB datasets are P-codes, which are unique
alphanumeric identifiers for each geographic region.

The COD ABs are downloaded from `HDX <https://data.humdata.org/>`_.


Usage
-----

To use this class, you first need to create a country configuration
for the country you would like to use:

.. code-block:: python

    from aatoolbox import create_country_config
    country_config = create_country_config(iso3="npl")

Next you need to instantiate the CodAB class with the country config:

.. code-block:: python

    from aatoolbox import CodAB
    codab = CodAB(country_config=country_config)

Upon first use, you will need to downlaod the COD AB data:

.. code-block:: python

    codab.download()

Finally, use the load method to begin working with the data as a
GeoPandas dataframe:

.. code-block:: python

    npl_admin1 = codab.load(admin_level=1)

The full code snippet is below in case you would like to copy it:

.. code-block:: python

    from aatoolbox import create_country_config, CodAB
    country_config = create_country_config(iso3="npl")
    codab = CodAB(country_config=country_config)
    codab.download()
    npl_admin1 = codab.load(admin_level=1)
