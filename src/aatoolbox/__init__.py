"""aa-toolbox: Toolbox for Anticipatory Action.

The ``aatoolbox`` library contains modules designed to assist with
downloading and processing the data required for disaster-related
anticipatory action.
"""
from subprocess import CalledProcessError

from aatoolbox.utils import git

from ._version import __version__  # noqa: F401

_MAIN_GIT_BRANCH = "main"

# For development: add commit hash to version number to enable
# upload of all builds to TestPyPI
try:
    if git.get_branch_name() != _MAIN_GIT_BRANCH:
        __version__ += f"-{git.get_short_hash()}"
except CalledProcessError:
    pass
