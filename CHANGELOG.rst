Changelog
=========

All notable changes to OCHA AnticiPy will be documented in this file.

The format is based on `Keep a
Changelog <https://keepachangelog.com/en/1.0.0/>`__, and this project
adheres to `Semantic
Versioning <https://semver.org/spec/v2.0.0.html>`__.

[1.0.0] - 2022-12-22
--------------------

Changed
~~~~~~~

- Renamed from AA Toolbox to OCHA AnticiPy

[0.5.0] - 2022-12-02
--------------------

Added
~~~~~

- Modules for downloading and processing CHIRPS rainfall,
  GloFAS river discharge, and USGS NDVI data
- Utilities to streamline use of strings for dates across modules
- COD AB configuration now has an ``admin{level}_name`` custom
  layer name parameter
- Logging for files being overwritten or not due to clobber

Changed
~~~~~~~

- Documented and moved the raster processing module to the top level
  for public access

Removed
~~~~~~~
- Python 3.6 support

Fixed
~~~~~
- The check in ``DataSource`` for the required configuration file
  section now also checks if the section is ``None``
- All available admin levels for DRC and Ethiopia are now accessible
- IRI download method now checks request headers to verify authentication

[0.4.2] - 2022-05-13
--------------------

Fixed
~~~~~

- Upgrade version of hdx-python-api to prevent bug when downloading


[0.4.1] - 2022-05-10
--------------------

Fixed
~~~~~

- Fixed error when loading zipped COD AB shapefiles on Windows

[0.4.0] - 2022-04-21
--------------------

Added
~~~~~

-  Implemented unit testing for ``AatRaster`` module with full coverage
-  Implemented downloading and processing for IRI seasonal precipitation
   forecast
-  Added config for DRC (of which the iso3 is COD)
-  User can now create a config object from a custom filepath
-  FewsNet region names are now part of config files
-  Documentation expanded and put on
   `ReadTheDocs <https://aa-toolbox.readthedocs.io/>`_

Changed
~~~~~~~

-  ``DataSource`` is now an abstract base class with required
   ``download``, ``process`` and ``load`` methods
-  ``GeoBoundingBox`` input parameters changed from ``north``,
   ``south``, ``east``, and ``west`` to ``lat_max``, ``lat_min``,
   ``lon_max``, ``lon_min``
-  ``GeoBoundingBox.round`` returns ``GeoBoundingBox`` instance (instead
   of being in place)
-  COD AB is now optional in the configuration file
-  FewsNet download functionality follows ``DataSource`` structure
-  This changelog converted from markdown to .rst

Removed
~~~~~~~

-  ``Pipeline`` class no longer used as main API
-  Removed unnecessary explicit install of test requirements on GitHub
   Actions

Fixed
~~~~~

-  GitHub action to publish on PyPI should not be invoked for pushes to
   main (using tags instead)
-  HDX API now uses “prod” server, and version >= 5.5.8 to avoid
   download error
-  COD AB dataset URLs on HDX are standardized
-  ``GeoBoundingBox`` won’t allow lat_max < lat_min or lon_max < lon_min
-  ``GeoBoundingBox`` imposes -90 < latitude < 90 and -180 < longitude <
   180

[0.3.1] - 2022-01-06
--------------------

Fixed
~~~~~

-  GitHub action to publish on PyPI when tagged was not running

[0.3.0] - 2022-01-06
--------------------

Added
~~~~~

-  ``Pipeline`` class to serve as main API
-  ``DataSource`` class as a base for all data sources
-  ``CodAB`` data source class for manipulating COD administrative
   boundaries
-  Functionality to download and save FewsNet data
-  Raster processing module
-  HDX API utility
-  Caching decorator in IO utility
-  Configuration files for:

   -  Bangladesh
   -  Ethiopia
   -  Malawi
   -  Nepal

-  `pip-compile <https://github.com/jazzband/pip-tools#version-control-integration>`__
   pre-commit hook to update requirements files
-  Version number is now specified in ``src/aatoolbox/_version.py``
-  GitHub actions to run unit tests (using ``tox.ini``) and push to PyPI

Changed
~~~~~~~

-  markdownlint pre-commit hook `switched to Node.js
   source <https://github.com/DavidAnson/markdownlint>`__
-  ``requirements.txt`` moved to ``requirements`` directory
-  ``Area`` class moved to utils
-  Switched from ``pbr`` to ``setuptools_scm`` for automated git tag
   versioning
-  Documentation to be generated using ``sphinx-build`` rather than
   through ``setup.py``

Removed
~~~~~~~

-  ``Makefile`` for generating requirements files
-  ``setup.py`` as it was only required for ``scm``

[0.2.1] - 2021-10-15
--------------------

Fixed
~~~~~

-  Version number reading function used wrong package name

[0.2.0] - 2021-10-15
--------------------

Added
~~~~~

-  Configuration class
-  Base class for data manipulation

[0.1.0] - 2021-10-12
--------------------

Added
~~~~~

-  CDS Area module
-  Package setup with PBR
-  Pre-commit hooks: black, flake8, mypy plus others
-  Sphinx documentation
