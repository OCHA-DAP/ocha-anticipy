"""Tests for check_file_existence decorator."""

import logging

import pytest

from ochanticipy.utils.check_file_existence import check_file_existence


@check_file_existence
def downloader(filepath, clobber):
    """I/O function for testing."""
    return "a"


def test_fp_exists_no_clobber(tmp_path, caplog):
    """Test that filepath returned if Path exists and clobber False."""
    caplog.set_level(logging.INFO)
    output_filepath = downloader(filepath=tmp_path, clobber=False)
    assert output_filepath == tmp_path
    assert (
        f"File {tmp_path} exists and clobber set to "
        "False, using existing file." in caplog.text
    )


def test_fp_exists_clobber(tmp_path, caplog):
    """Test that function returned if Path exists and clobber True."""
    caplog.set_level(logging.INFO)
    output_filepath = downloader(filepath=tmp_path, clobber=True)
    assert output_filepath == "a"
    assert (
        f"File {tmp_path} exists and clobber set to "
        "True, overwriting existing file." in caplog.text
    )


def test_fp_not_exists(tmp_path, caplog):
    """Test that function returned if Path does not exist."""
    caplog.set_level(logging.INFO)
    path_new = tmp_path / "new_path"
    output_filepath = downloader(filepath=path_new, clobber=False)
    assert output_filepath == "a"
    assert (
        f"File {path_new} does not exist and clobber set to "
        "False, downloading new file." in caplog.text
    )


def test_key_error(tmp_path):
    """Test that KeyError raised when filepath or clobber not a kwarg."""
    with pytest.raises(KeyError):
        downloader(filepath=tmp_path)
        downloader(clobber=True)
        downloader()
