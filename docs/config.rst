Country Configuration
=====================

Before executing any components of the pipeline, you'll need to create
a country configuration object. These objects are derived from either
built-in or user provided YAML files, and specify general country information
such as the ISO3, in addition to country- and data-source specific
information, such as the COD AB shapefile layer names.

Built-in Country Configuration
------------------------------

Several countries are already supported out-of-the box,
and we are continuously adding more.

.. _list of supported countries:

We currently support all 25 humanitarian response plan (HRP) countries:

- Afghanistan (AFG)
- Burkina Faso (BFA)
- Burundi (BDI)
- Cameroon (CMR)
- Central African Republic (CAF)
- Chad (TCD)
- Colombia (COL)
- Democratic Republic of the Congo (COD)
- Ethiopia (ETH)
- Haiti (HTI)
- Iraq (IRQ)
- Libya (LBY)
- Mali (MLI)
- Mozambique (MOZ)
- Myanmar (MMR)
- Niger (NER)
- Nigeria (NGA)
- Somalia (SOM)
- South Sudan (SSD)
- State of Palestine (PSE)
- Sudan (SDN)
- Syrian Arab Republic (SYR)
- Ukraine (UKR)
- Venezuela (VEN)
- Yemen (YEM)

In addition we support several non-HRP countries:

- Bangladesh (BGD)
- Malawi (MWI)
- Nepal (NPL)

Please contact us
or create an issue if you would like to see more countries
added to this list.

To create a country configuration object for one of the supported countries,
you first need to know the country's
`ISO3 <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3#Officially_assigned_code_elements>`_.
The example below shows how to create a country configuration object
for Nepal, which has the ISO3 "NPL":

.. code-block:: python

    from ochanticipy import create_country_config
    country_config = create_country_config(iso3="npl")


Unavailable Data
----------------

For certain countries, data from a specific source may be unavailable. For the HRP countries,
Afghanistan and Myanmar do not have COD AB configurations, as Afghanistan's COD AB shapefiles
are private and Myanmar does not have COD AB files. In these cases, you will need to manually
load and use other files.


Custom Country Configuration
----------------------------

If you would like to work with a country that we do not yet support,
or would like to make modifications to our implementation, you can
create a custom country configuration by creating your own
`YAML <https://en.wikipedia.org/wiki/YAML>`_
file and passing the filename to the custom constructor:

.. code-block:: python

    from ochanticipy import create_custom_country_config

    filename = "/path/to/filename"
    country_config = create_custom_country_config(filename=filename)

The YAML file will be validated and used to create the country configuration.
At minimum, it must contain the country ISO3:

.. code-block:: yaml

    iso3: "npl"

For additional datasource-specific requirements, please see the
documentation for each datasource.
