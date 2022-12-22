# OCHA AnticiPy: Access data for anticipating humanitarian risk

[![license](https://img.shields.io/github/license/OCHA-DAP/ocha-anticipy.svg)](https://github.com/OCHA-DAP/pa-aa-toolbx/blob/main/LICENSE)
[![Test Status](https://github.com/OCHA-DAP/ocha-anticipy/workflows/tests/badge.svg)](https://github.com/OCHA-DAP/ocha-anticipy/actions?query=workflow%3Atests)
[![PyPI Status](https://github.com/OCHA-DAP/ocha-anticipy/workflows/PyPI/badge.svg)](https://github.com/OCHA-DAP/ocha-anticipy/actions?query=workflow%3APyPI)
[![Documentation Status](https://readthedocs.org/projects/ocha-anticipy/badge/?version=latest)](https://ocha-anticipy.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://codecov.io/gh/OCHA-DAP/ocha-anticipy/branch/main/graph/badge.svg?token=JpWZc5js4y)](https://codecov.io/gh/OCHA-DAP/ocha-anticipy)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

OCHA AnticiPy is a Python library for simple downloading and processing
of data related to the anticipation of humanitarian risk,
from climate observations and forecasts to food insecurity.

The datasets that we currently support are:

- CHIRPS rainfall
- COD ABs (Common Operational Datasets administrative boundaries)
- FEWS NET food insecurity
- GloFAS river discharge
- IRI seasonal rainfall forecast
- USGS NDVI (normalized difference vegetation index)

For more information, please see the
[documentation](https://ocha-anticipy.readthedocs.io/en/latest/).

## Installing

Install and update using [pip](https://pip.pypa.io/en/stable/getting-started/):

```shell
pip install ocha-anticipy
```

## A Simple Example

OCHA AnticiPy downloads data to the directory referenced by the
environment variable `OAP_DATA_DIR`. Before beginning, please make
sure that this environment variable is defined and points to where you would
like the data to go.

Next, you can simply download the admin boundary CODs for Nepal,
and retrieve provinces as a GeoDataFrame:

```python
from ochanticipy import create_country_config, CodAB

country_config = create_country_config('npl')
codab = CodAB(country_config=country_config)
codab.download()
provinces = codab.load(admin_level=1)
```

## Contributing

For guidance on setting up a development environment, see the
[contributing guidelines](https://github.com/OCHA-DAP/ocha-anticipy/blob/main/CONTRIBUTING.rst)

## Links

- [Documentation](https://ocha-anticipy.readthedocs.io/en/latest/)
- [Changes](https://github.com/OCHA-DAP/ocha-anticipy/blob/main/CHANGELOG.rst)
- [PyPI Releases](https://pypi.org/project/ocha-anticipy/)
- [Source Code](https://github.com/OCHA-DAP/ocha-anticipy)
- [Issue Tracker](https://github.com/OCHA-DAP/ocha-anticipy/issues)
