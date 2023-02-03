Development
===========

Environment
-----------

Development is currently done using Python 3.11. We recommend using a virtual
environment such as ``venv``:

.. code:: shell

    python3.11 -m venv venv
    source venv/bin/activate

In your virtual environment, please install all packages from
``requirements/requirements-dev.txt``:

.. code:: shell

   pip install -r requirements/requirements-dev.txt

OCHA AnticiPy makes use of
`geopandas <https://geopandas.org/en/stable/>`__, which depends on
`Fiona <https://github.com/Toblerity/Fiona>`__, so you will need to
have `GDAL <https://github.com/Toblerity/Fiona#installation>`__
installed.

Installation
------------

To install in editable mode for development, execute:

.. code:: shell

   pip install -e .

Testing
-------

To run the tests and view coverage, execute:

.. code:: shell

   python -m pytest --cov=ochanticipy

Directly running pytest may
[produce errors](https://stackoverflow.com/questions/40718770/pytest-running-with-another-version-of-python).
by testing using a Python version or environment different from
the local development environment, hence we recommend running
as above.

Documentation
-------------

Docstrings
^^^^^^^^^^

All public modules, classes and methods should be documented with
`numpy-style <https://numpydoc.readthedocs.io/en/latest/format.html>`__
docstrings.

API documentation structure
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The API documentation structure is defined in ``api.rst``. The structure
pulls from the docstrings for the modules, classes, and methods using
Sphinx autodocumentation. ReST documentation is used to create navigable
structure.

Sphinx autodocumentation
""""""""""""""""""""""""

For a module and its children, the structure should follow the below guidelines.

1. Document each module in the ``.py`` file itself. This is then documented
directly using ``aautomodule``.

2. Below this, document classes or functions as necessary. For classes
that inherit directly from ``DataSource`` or other low-level base classes,
just document ``:members:`` of the class. This ensures that only methods
defined specifically for this class are documented.

3. For classes that inherit from a base class that defines specific methods,
like ``ChirpsDaily`` from ``_Chirps``, specify ``:inherited-members:`` as
well. This ensures that only methods defined in the base class are documented.

4. For something like the raster module classes, which inherits methods from
a base class, but also defines unique methods within the subclass, use both
``:members:`` and ``:inherited-members:``.

Follow the example set out already in ``api.rst`` as you write the documentation.

Module order
""""""""""""

The ordering of the ``api.rst`` documentation should match the ordering
specified in the ``__all__`` list ``src/ochanticipy/__init__.py``.

Build and view
^^^^^^^^^^^^^^

To build the documentation and test your implementation, use the following command:

.. code:: shell

   sphinx-build -b html -d docs/build/doctrees docs/source docs/build/html

To view the docs, open up ``docs/build/html/index.html`` in your
browser.

pre-commit
----------

All code is formatted according to
`black <https://github.com/psf/black>`__ and
`flake8 <https://flake8.pycqa.org/en/latest/>`__ guidelines. The repo is
set-up to use `pre-commit <https://github.com/pre-commit/pre-commit>`__.
So please run ``pre-commit install`` the first time you are editing.
Thereafter all commits will be checked against black and flake8
guidelines

To check if your changes pass pre-commit without committing, run:

.. code:: shell

   pre-commit run --all-files

Packages
--------

`pip-tools <https://github.com/jazzband/pip-tools>`__ is used for
package management.

If you’ve introduced a new package to the source code (i.e. anywhere in
``src/``), please add it to the ``install_requires`` section of
``setup.cfg`` with any known version constraints. For adding packages
for development, documentation, or tests, add them to the relevant
``.in`` file in the ``requirements`` directory. When you modify any of
these lists, please try to keep them alphabetical! Any changes to the
``requirements*.txt`` files will be generated with ``pre-commit``.

To run this without commiting, execute:

.. code:: shell

   pre-commit run pip-compile --all-files

For other functionality such as updating specific package versions,
refer to the ``pip-tools`` documentation.

Package Release
---------------

Features are developed on our ``develop`` branch, with changes tracked
in the “Unreleased” section at the top of ``CHANGELOG.md``. Upon
release, the ``develop`` branch is merged to ``main`` and the release is
tagged according to `semantic
versioning <https://semver.org/spec/v2.0.0.html>`__.

Versioning is handled by
`setuptools_scm <https://github.com/pypa/setuptools_scm>`__, and the
configuration for this can be found in ``pyproject.toml``

The OCHA AnticiPy package is built and published to
`PyPI <https://pypi.org/project/ocha-anticipy/>`__ whenever a new tag is
pushed. With each new commit, a development version is pushed to
`TestPyPI <https://test.pypi.org/project/ocha-anticipy>`__ and is available
to install for testing purposes by running:

.. code:: shell

   pip install --index-url https://test.pypi.org/simple/
   --extra-index-url https://pypi.org/simple ocha-anticipy==$VERSION
