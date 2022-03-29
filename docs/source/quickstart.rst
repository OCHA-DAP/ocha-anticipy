Quickstart
==========

Setup and installation
----------------------

There are two main steps to get up and running:

Setup environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^

AA Toolbox downloads data to the directory referenced by the
environment variable `AA_DATA_DIR`. Before beginning, please make
sure that this environment variable points to where you would
like the data to go.

Install AA Toolbox
^^^^^^^^^^^^^^^^^^

AA Toolbox supports Python 3.6 and newer. It can be installed in your Python
environment using the following command:

.. code-block:: sh

    $ pip install aa-toolbox

AA Toolbox should now be installed.


COD administrative boundaries
-----------------------------

A simple dataset to get started with are the COD administrative boundaries.
With the following code, you can download COD data for Nepal from HDX,
and load the provinces as a GeoDataFrame.

.. code-block:: python

    from aatoolbox import create_country_config, CodAB

    nepal_config = create_country_config('npl')

    nepal_codab = CodAB(country_config=nepal_config)
    nepal_codab.download()

    nepal_provinces = nepal_codab.load(admin_level=1)

What does this code do?

1.  First we import the :func:`~aatoolbox.create_country_config` function and
    the :class:`~aatoolbox.CodAB` class. The function will be used to setup
    the country of interest, and an instance of the CodAB class will
    be used to download the data.
2.  Next we create a country configuration object specific to Nepal
    (which has 'npl' as its ISO3) using the
    :func:`~aatoolbox.create_country_config` function.
3.  We then create an instance of the :class:`~aatoolbox.CodAB` class
    using the Nepal-specific country configuration instance.
4.  From the CodAB instance, we are then able to download the COD
    administrative boundaries for Nepal. These are placed in the
    ``AA_DATA_DIR`` directory.
5.  Finally, we can use the CodAB instance to load a specific administrative
    boundary level. In this case we are loading level 1 which corresponds
    to provinces in Nepal.
