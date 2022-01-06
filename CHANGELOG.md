<!-- markdownlint-disable-file MD024 -->

# Changelog

All notable changes to `aa-toolbox` will be documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2022-01-06

### Fixed

- GitHub action to publish on PyPI when tagged was not running

## [0.3.0] - 2022-01-06

### Added

- `Pipeline` class to serve as main API
- `DataSource` class as a base for all data sources
- `CodAB` data source class for manipulating COD administrative boundaries
- Functionality to download and save FewsNet data
- Raster processing module
- HDX API utility
- Caching decorator in IO utility
- Configuration files for:
  - Bangladesh
  - Ethiopia
  - Malawi
  - Nepal
- [`pip-compile`](https://github.com/jazzband/pip-tools#version-control-integration)
  pre-commit hook to update requirements files
- Version number is now specified in `src/aatoolbox/_version.py`
- GitHub actions to run unit tests (using `tox.ini`) and push to PyPI

### Changed

- markdownlint pre-commit hook [switched to Node.js source](https://github.com/DavidAnson/markdownlint)
- `requirements.txt` moved to `requirements` directory
- `Area` class moved to utils
- Switched from `pbr` to `setuptools_scm` for automated git tag versioning
- Documentation to be generated using `sphinx-build` rather than through `setup.py`

### Removed

- `Makefile` for generating requirements files
- `setup.py` as it was only required for `scm`

## [0.2.1] - 2021-10-15

### Fixed

- Version number reading function used wrong package name

## [0.2.0] - 2021-10-15

### Added

- Configuration class
- Base class for data manipulation

## [0.1.0] - 2021-10-12

### Added

- CDS Area module
- Package setup with PBR
- Pre-commit hooks: black, flake8, mypy plus others
- Sphinx documentation
