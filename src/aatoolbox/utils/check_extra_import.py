"""Check imports that are in extras_require."""
import importlib


def _check_extra_import(library: str, subpackage: str):
    """Import library or error if not installed.

    Parameters
    ----------
    library : str
        String of library to import.
    subpackage : str
        String of subpackage defined for extra_requires
        that import should warn to install from.
    """
    try:
        return importlib.import_module(library)
    except ModuleNotFoundError as err:
        raise ModuleNotFoundError(
            f"{library} library is not found. Install it "
            f"using `pip install aa-toolbox[{subpackage}]`."
        ) from err
