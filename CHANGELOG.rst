.. raw:: html

Changelog
=========

All notable changes to ``aa-toolbox`` will be documented in this file.

The format is based on `Keep a
Changelog <https://keepachangelog.com/en/1.0.0/>`__, and this project
adheres to `Semantic
Versioning <https://semver.org/spec/v2.0.0.html>`__.

[Unreleased]
------------

Added
~~~~~

-  Implemented unit testing for ``AatRaster`` module with full coverage
-  Implemented downloading and processing for IRI seasonal precipitation
   forecast
-  Added config for DRC (of which the iso3 is COD)

Changed
~~~~~~~

-  ``DataSource`` is now an abstract base class with required
   ``download``, ``process`` and ``load`` methods

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

[0.3.1] - 2022-01-06
--------------------

.. _fixed-1:

Fixed
~~~~~

-  GitHub action to publish on PyPI when tagged was not running

.. _section-1:

[0.3.0] - 2022-01-06
--------------------

.. _added-1:

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

-  ```pip-compile`` <https://github.com/jazzband/pip-tools#version-control-integration>`__
   pre-commit hook to update requirements files
-  Version number is now specified in ``src/aatoolbox/_version.py``
-  GitHub actions to run unit tests (using ``tox.ini``) and push to PyPI

.. _changed-1:

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

.. _removed-1:

Removed
~~~~~~~

-  ``Makefile`` for generating requirements files
-  ``setup.py`` as it was only required for ``scm``

.. _section-2:

[0.2.1] - 2021-10-15
--------------------

.. _fixed-2:

Fixed
~~~~~

-  Version number reading function used wrong package name

.. _section-3:

[0.2.0] - 2021-10-15
--------------------

.. _added-2:

Added
~~~~~

-  Configuration class
-  Base class for data manipulation

.. _section-4:

[0.1.0] - 2021-10-12
--------------------

.. _added-3:

Added
~~~~~

-  CDS Area module
-  Package setup with PBR
-  Pre-commit hooks: black, flake8, mypy plus others
-  Sphinx documentation
