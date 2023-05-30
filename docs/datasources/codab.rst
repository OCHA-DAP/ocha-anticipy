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

The COD ABs are downloaded from `HDX <https://data.humdata.org/cod>`_.


Usage
-----

To use this class, you first need to create a country configuration
for the country you would like to use:

.. code-block:: python

    from ochanticipy import create_country_config
    country_config = create_country_config(iso3="nga")

Next you need to instantiate the CodAB class with the country config:

.. code-block:: python

    from ochanticipy import CodAB
    codab = CodAB(country_config=country_config)

Upon first use, you will need to downlaod the COD AB data:

.. code-block:: python

    codab.download()

Finally, use the load method to begin working with the data as a
GeoPandas dataframe:

.. code-block:: python

    nga_admin1 = codab.load(admin_level=1)

Some COD AB files have additional layers that don't correspond to
an admin level. For example, Nigeria has a districts layer, which
is provided in the config file as the first custom layer:

.. code-block:: python

    nga_districts = codab.load_custom(custom_layer_number=0)

The full code snippet is below in case you would like to copy it:

.. code-block:: python

    from ochanticipy import create_country_config, CodAB
    country_config = create_country_config(iso3="nga")
    codab = CodAB(country_config=country_config)
    codab.download()
    nga_admin1 = codab.load(admin_level=1)
    nga_districts = codab.load_custom(custom_layer_number=0)

Configuration
-------------

The COD AB portion of the configuration file
should be setup as follows:

.. code-block:: yaml

    codab:
        hdx_resource_name:
            - nga_adm_osgof_20190417.zip
            - nga_additional.zip # contrived example for documentation
        layer_base_name: nga_admbnda_adm{admin_level}_osgof_20190417.shp
        admin_level_max: 3
        adm1_name: nga_adm1.shp # contrived example for documentation
        custom_layer_names:
            - nga_admbnda_senDist_inec_osgof_20190417.shp

Below is an explanation of the different parameters:

``hdx_resource_name``: The name of the shapefile resource on HDX. It can be found by taking
the filename as it appears on the HDX page. For example, you can see on the
`page for Nigeria <https://data.humdata.org/dataset/cod-ab-nga>`_ that the shapefile
(sometimes with the ``.shp``. or ``.SHP`` extension, this time not) has the name
``nga_adm_osgof_20190417.zip``. If admin levels are stored in different resources, which is
the case for some countries like Niger, then each individual resource should should be listed
under ``hdx_resource_name``. The format is shown above as an example to follow.

``layer_base_name``: The baseline name of the different admin level layers, with the
level number replaced by the variable ``{admin_level}``. To find this, you will need
to open up the shpaefile in e.g. `QGIS <https://www.qgis.org/en/site/>`_.
In the case of Nigeria, the layers have the names ``nga_admbnda_adm0_osgof_20190417.shp``,
``nga_admbnda_adm1_osgof_20190417.shp``, and ``nga_admbnda_adm2_osgof_20190417.shp``

``admin_level_max``: The maximum admin level available in the layers. In the case of Nigeria,
the layer level numbers range from 0 to 3, so the maximum should be 3. In general the
 maximum admin level should not exceed 4.

``admin{level}_name``: An optional parameter for any admin level (``level`` can range from 0 to 4)
whose layer names do not match the ``layer_base_name`` pattern. This example for Nigeria
 is contrived, but this issue does exist for COD ABs from countries such as Ethiopia and DRC.

``custom_layer_name``: A place to list any other layers that don't correspond to the
admin level format specified above. In the case of Nigeria, there is a layer for districts
with the name ``nga_admbnda_senDist_inec_osgof_20190417.shp``.
