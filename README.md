# aa-toolbox: Toolbox for anticipatory action

[![license](https://img.shields.io/github/license/OCHA-DAP/pa-aa-toolbox.svg)](https://github.com/OCHA-DAP/pa-aa-toolbx/blob/main/LICENSE)
[![Test Status](https://github.com/OCHA-DAP/pa-aa-toolbox/workflows/tests/badge.svg)](https://github.com/OCHA-DAP/pa-aa-toolbox/actions?query=workflow%3Atests)
[![PyPI Status](https://github.com/OCHA-DAP/pa-aa-toolbox/workflows/PyPI/badge.svg)](https://github.com/OCHA-DAP/pa-aa-toolbox/actions?query=workflow%3APyPI)
[![Documentation Status](https://readthedocs.org/projects/aa-toolbox/badge/?version=latest)](https://aa-toolbox.readthedocs.io/en/latest/?badge=latest)
[![Coverage Status](https://codecov.io/gh/OCHA-DAP/pa-aa-toolbox/branch/main/graph/badge.svg?token=JpWZc5js4y)](https://codecov.io/gh/OCHA-DAP/pa-aa-toolbox)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

The [anticipatory action](https://centre.humdata.org/anticipatory-action/)
(AA) toolbox is a Python package to support development of AA frameworks, by
simplifying the downloading and processing of commonly used datasets.

The datasets that we currently support are:

- CHIRPS rainfall
- COD ABs (Common Operational Datasets administrative boundaries)
- FEWS NET food insecurity
- GloFAS river discharge
- IRI seasonal rainfall forecast
- USGS NDVI (normalized difference vegetation index)

For more information, please see the
[documentation](https://aa-toolbox.readthedocs.io/en/latest/).

## Installing

Install and update using [pip](https://pip.pypa.io/en/stable/getting-started/):

```shell
pip install -U aa-toolbox
```

## A Simple Example

Download the admin boundary CODs for Nepal, and retrieve provinces
as a GeoDataFrame:

```python
from aatoolbox import create_country_config, CodAB

country_config = create_country_config('npl')
codab = CodAB(country_config=country_config)
codab.download()
provinces = codab.load(admin_level=1)
```

## Contributing

For guidance on setting up a development environment, see the
[contributing guidelines](https://github.com/OCHA-DAP/pa-aa-toolbox/blob/main/CONTRIBUTING.rst)

## Links

- [Documentation](https://aa-toolbox.readthedocs.io/en/latest/)
- [Changes](https://github.com/OCHA-DAP/pa-aa-toolbox/blob/main/CHANGELOG.rst)
- [PyPI Releases](https://pypi.org/project/aa-toolbox/)
- [Source Code](https://github.com/OCHA-DAP/pa-aa-toolbox)
- [Issue Tracker](https://github.com/OCHA-DAP/pa-aa-toolbox/issues)
