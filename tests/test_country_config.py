"""Tests for the country configuration."""
import copy
from typing import Dict, List

import pytest

from aatoolbox import create_country_config


@pytest.fixture
def mock_parse_yaml(mocker):
    """Fixture to return custom dict from yaml parser."""

    def patcher(output_list: List[Dict] = None):
        if output_list is None:
            output_list = [{"iso3": "abc"}]
        return mocker.patch(
            "aatoolbox.config.countryconfig.parse_yaml",
            side_effect=output_list,
        )

    return patcher


def test_create_country_config(mock_parse_yaml):
    """Test that country config is created as expected."""
    mock_parse_yaml()
    country_config = create_country_config(iso3="abc")
    assert country_config.iso3 == "abc"


def test_country_not_implemented():
    """Test error raised when country not implemented."""
    with pytest.raises(FileNotFoundError):
        create_country_config(iso3="abc")


def test_input_iso3():
    """Test that user input iso3 is validated, but can be uppercase."""
    bad_iso3s = ["abcd", "ab", "123"]
    with pytest.raises(ValueError):
        for iso3 in bad_iso3s:
            create_country_config(iso3=iso3)


def test_uppercase_input_iso3(mock_parse_yaml):
    """Test that uppercase iso3 converted to lowercase for filename."""
    fake_parser = mock_parse_yaml()
    create_country_config(iso3="ABC")
    _, args, _ = fake_parser.mock_calls[0]
    assert "abc.yaml" in str(args[0])


def test_config_iso3(mock_parse_yaml):
    """Test that iso3 in config file is validated."""
    bad_iso3s = ["ABC", "abcd", "ab", "123"]
    mock_parse_yaml(output_list=[{"iso3": iso3} for iso3 in bad_iso3s])
    with pytest.raises(ValueError):
        for _ in bad_iso3s:
            create_country_config(iso3="abc")


def test_validate_codab_layer_base_name(mock_parse_yaml):
    """Test that layer basename requires correct placeholder."""
    config_base = {
        "iso3": "abc",
        "codab": {
            "hdx_dataset_name": "fake_dataset_name",
            "layer_base_name": "layer_base_name_",
            "admin_level_max": 1,
        },
    }
    config_correct = copy.deepcopy(config_base)
    config_correct["codab"]["layer_base_name"] += "{admin_level}"
    config_incorrect = copy.deepcopy(config_base)
    config_incorrect["codab"]["layer_base_name"] += "{wrong_placeholder}"
    mock_parse_yaml(output_list=[config_correct, config_incorrect])
    # Check that correct config runs without issue
    create_country_config(iso3="abc")
    # Check that incorrect config raises error
    with pytest.raises(ValueError):
        create_country_config(iso3="abc")
