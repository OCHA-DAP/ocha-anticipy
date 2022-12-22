Quickstart
==========

Setup and installation
----------------------

There are two main steps to get up and running:

Setup environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

OCHA AnticiPy downloads data to the directory referenced by the
environment variable `OAP_DATA_DIR`. Before beginning, please make
sure that this environment variable is defined and points to where you would
like the data to go.

Install OCHA AnticiPy
^^^^^^^^^^^^^^^^^^

OCHA AnticiPy supports Python 3.7 and newer. It can be installed in your Python
environment using the following command:

.. code-block:: sh

    $ pip install ocha-anticipy

OCHA AnticiPy should now be installed.

Usage Examples
--------------

COD administrative boundaries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A simple dataset to get started with are administrative boundaries,
which are one of the
`Common Operational Datasets (CODs) on HDX <https://data.humdata.org/cod>`_.
With the following code, you can download COD data for Nepal from HDX,
and load the provinces as a GeoDataFrame.

.. code-block:: python

    from ochanticipy import create_country_config, CodAB

    nepal_config = create_country_config(iso3='npl')

    nepal_codab = CodAB(country_config=nepal_config)
    nepal_codab.download()

    nepal_provinces = nepal_codab.load(admin_level=1)

What does this code do?

1.  First we import the :func:`~ochanticipy.create_country_config` function and
    the :class:`~ochanticipy.CodAB` class. The function will be used to setup
    the country of interest, and an instance of the CodAB class will
    be used to download the data.
2.  Next we create a country configuration object specific to Nepal
    by providing as input its ISO3 (which is 'npl'), using the
    :func:`~ochanticipy.create_country_config` function.
3.  We then create an instance of the :class:`~ochanticipy.CodAB` class
    using the Nepal-specific country configuration instance.
4.  From the CodAB instance, we are then able to call `download()`, which
    downloads the COD
    administrative boundaries for Nepal. These are placed in the directory where
    the environment variable named ``OAP_DATA_DIR`` points to.
5.  Finally, we can use the CodAB instance to load a specific administrative
    boundary level. In this case we are loading level 1 which corresponds
    to provinces in Nepal.
