# aa-toolbox: Toolbox for anticipatory action

[![license](https://img.shields.io/github/license/OCHA-DAP/pa-aa-toolbox.svg)](https://github.com/OCHA-DAP/pa-aa-toolbx/blob/main/LICENSE)
[![Test Status](https://github.com/OCHA-DAP/pa-aa-toolbox/workflows/tests/badge.svg)](https://github.com/OCHA-DAP/pa-aa-toolbox/actions?query=workflow%3Atests)
[![PyPI Status](https://github.com/OCHA-DAP/pa-aa-toolbox/workflows/PyPI/badge.svg)](https://github.com/OCHA-DAP/pa-aa-toolbox/actions?query=workflow%3APyPI)
[![Coverage Status](https://codecov.io/gh/OCHA-DAP/pa-aa-toolbox/branch/main/graph/badge.svg?token=JpWZc5js4y)](https://codecov.io/gh/OCHA-DAP/pa-aa-toolbox)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

Currently under development

## Development

### Environment

To set up the development environment, please install all packages
from `requirements/requirements-dev.txt`:

```shell
pip install -r requirements/requirements-dev.txt
```

`aa-toolbox` makes use of [`geopandas`](https://geopandas.org/en/stable/),
which depends on [`Fiona`](https://github.com/Toblerity/Fiona),
so you will need to have [`GDAL`](https://github.com/Toblerity/Fiona#installation)
installed.

### Installation

To install in editable mode for development, execute:

```shell
pip install -e .
```

### Testing

To run the tests and view coverage, execute:

```shell
pytest --cov=aatoolbox
```

### Documentation

All public modules, classes and methods should be documented with
[numpy-style](https://numpydoc.readthedocs.io/en/latest/format.html)
docstrings, which can be rendered into HTML using the following command:

```shell
sphinx-build -b html -d docs/build/doctrees docs/source docs/build/html
```

To view the docs, open up `docs/build/html/index.html` in your browser.

### pre-commit

All code is formatted according to
[black](https://github.com/psf/black) and
[flake8](https://flake8.pycqa.org/en/latest/) guidelines.
The repo is set-up to use
[pre-commit](https://github.com/pre-commit/pre-commit).
So please run `pre-commit install` the first time you are editing.
Thereafter all commits will be checked against black and flake8 guidelines

To check if your changes pass pre-commit without committing, run:

```shell
pre-commit run --all-files
```

### Packages

[`pip-tools`](https://github.com/jazzband/pip-tools)
is used for package management.

If you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `install_requires` section of `setup.cfg` with any known
version constraints.
For adding packages for development, documentation, or tests,
add them to the relevant `.in` file in the `requirements` directory.
When you modify any of these lists, please try to keep them alphabetical!
Any changes to the `requirements*.txt` files will be generated with `pre-commit`.

To run this without commiting, execute:

```shell
pre-commit run pip-compile --all-files
```

For other functionality such as updating specific package versions, refer to the
`pip-tools` documentation.

### Package Release

Features are developed on our `develop` branch, with changes tracked in the
"Unreleased" section at the top of `CHANGELOG.md`. Upon release, the `develop`
branch is merged  to `main` and the release is tagged according to
[semantic versioning](https://semver.org/spec/v2.0.0.html).

Versioning is handled by
[`setuptools_scm`](https://github.com/pypa/setuptools_scm),
and the configuration for this can be found in `pyproject.toml`

The `aa-toolbox` package is built and published to
[PyPI](https://pypi.org/project/aa-toolbox/)
whenever a new tag is pushed.
With each new commit, a development version is pushed to
[TestPyPI](https://test.pypi.org/project/aa-toolbox)
and is available to install for testing purposes by running:

```shell
pip install -i https://test.pypi.org/simple/ aa-toolbox==$VERSION
```
