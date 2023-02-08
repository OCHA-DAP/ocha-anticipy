"""Tests for check_file_existence decorator."""

import pytest

from ochanticipy.utils.check_extra_imports import check_extra_imports


def test_import_missing():
    """Test error raised for missing import."""
    with pytest.raises(ModuleNotFoundError, match=r"ochanticipy\[a\]"):
        check_extra_imports(libraries=["asdfasdf"], subpackage="a")


def test_import_present():
    """Test no error for available libraries."""
    check_extra_imports(libraries=["math", "numpy"], subpackage="a")
