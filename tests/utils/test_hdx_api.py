"""Tests for HDX API utility."""
from collections import UserDict

import pytest

from ochanticipy.utils.hdx_api import load_dataset_from_hdx


@pytest.fixture(autouse=True)
def mock_resource(mocker):
    """Mock the HDX download function."""
    # Resource is a UserDict so need to make a class to mock that
    class MockResource(UserDict):
        def download(self, folder):
            return "", "resource_filepath"

    mock_dataset = mocker.patch(
        "ochanticipy.utils.hdx_api.Dataset.read_from_hdx"
    )
    # read_from_hdx creates an instance, need to mock the instance
    # method get_resources to return custom resources
    mock_dataset.return_value.get_resources.return_value = [
        MockResource({"name": "resource1"})
    ]
    # Also need to mock out shutil
    mocker.patch("ochanticipy.utils.hdx_api.shutil")


def test_returns_filepath(tmp_path):
    """Test that querying HDX API returns expected filepath."""
    input_filepath = tmp_path / "hdx_test_path"
    output_filepath = load_dataset_from_hdx(
        hdx_address="hdx_address",
        hdx_dataset_name="resource1",
        output_filepath=input_filepath,
    )
    assert output_filepath == input_filepath


def test_error_when_not_found(tmp_path):
    """Test that missing resource raises error."""
    with pytest.raises(FileNotFoundError):
        load_dataset_from_hdx(
            hdx_address="hdx_address",
            hdx_dataset_name="some_name_not_in_fake_resrouce",
            output_filepath=tmp_path / "hdx_test_error",
        )
