<!-- markdownlint-disable-file MD024 -->

# Changelog

All notable changes to `aa-tools` will be documented in this file.

The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Pre-commit hook to check Sphinx documentation can be built
- [`pip-compile`](https://github.com/jazzband/pip-tools#version-control-integration)
  pre-commit hook to update requirements files
- version number is now specified in `src/aatoolbox/_version.py`

### Changed

- markdownlint pre-commit hook [switched to Node.js source](https://github.com/DavidAnson/markdownlint)
- `requirements.txt` moved to `requirements` directory

### Removed

- `pbr` for automated git tag versioning (which also allowed the removal
  of `setup.py`)
- `Makefile` for generating requirements files

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
