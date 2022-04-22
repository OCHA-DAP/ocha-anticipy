"""Tests for check_file_existence decorator."""

import pytest

from aatoolbox.utils.check_file_existence import check_file_existence


@check_file_existence
def downloader(filepath, clobber):
    """I/O function for testing."""
    return "a"


def test_fp_exists_no_clobber(tmp_path):
    """Test that filepath returned if Path exists and clobber False."""
    tmp_path.touch()
    output_filepath = downloader(filepath=tmp_path, clobber=False)
    assert output_filepath == tmp_path


def test_fp_exists_clobber(tmp_path):
    """Test that function returned if Path exists and clobber True."""
    tmp_path.touch()
    output_filepath = downloader(filepath=tmp_path, clobber=True)
    assert output_filepath == "a"


def test_fp_not_exists(tmp_path):
    """Test that function returned if Path does not exist."""
    output_filepath = downloader(filepath=tmp_path, clobber=True)
    assert output_filepath == "a"


def test_key_error(tmp_path):
    """Test that KeyError raised when filepath or clobber not a kwarg."""
    with pytest.raises(KeyError):
        downloader(filepath=tmp_path)
        downloader(clobber=True)
        downloader()
