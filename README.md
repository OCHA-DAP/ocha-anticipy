# aa-toolbox: Toolbox for anticipatory action

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

Currently under development

## Development

### pre-commit
All code is formatted according to
[black](https://github.com/psf/black) and
[flake8](https://flake8.pycqa.org/en/latest/) guidelines.
The repo is set-up to use
[pre-commit](https://github.com/pre-commit/pre-commit).
So please run `pre-commit install` the first time you are editing.
Thereafter all commits will be checked against black and flake8 guidelines

To check if your changes pass pre-commit without committing, run:
```buildoutcfg
pre-commit run --all-files
```

### Installation

To install, execute:
```buildoutcfg
python setup.py develop
```

### Testing

To run the tests and view coverage, execute:
```buildoutcfg
pytest --cov=aatolbox
```

### Documentation

To build the docs, execute:
```buildoutcfg
python setup.py build_sphinx
```
To view the docs, open up `docs/build/html/index.html` in your browser.
