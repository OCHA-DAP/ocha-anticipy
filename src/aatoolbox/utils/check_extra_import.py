"""Check imports that are in extras_require."""
import importlib.util


def _check_extra_imports(libraries: list, subpackage: str):
    """Import library or error if not installed.

    Parameters
    ----------
    libraries : str
        List of libraries to check.
    subpackage : str
        String of subpackage defined for extra_requires
        that import should warn to install from.
    """
    missing_pkgs = []
    for library in libraries:
        if importlib.util.find_spec(library) is None:
            missing_pkgs += [library]

    if missing_pkgs:
        raise ModuleNotFoundError(
            f"{', '.join(missing_pkgs)} were not found. Install the "
            f"missing packages with `pip install aa-toolbox[{subpackage}]`."
        )
