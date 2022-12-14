"""Tests for the country configuration."""
import copy
from typing import Dict, List

import pytest

from ochanticipy import create_country_config
from ochanticipy.config.countryconfig import FewsNetConfig


@pytest.fixture
def mock_parse_yaml(mocker):
    """Fixture to return custom dict from yaml parser."""

    def patcher(output_list: List[Dict] = None):
        if output_list is None:
            output_list = [{"iso3": "abc"}]
        return mocker.patch(
            "ochanticipy.config.countryconfig.parse_yaml",
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


@pytest.fixture
def bad_iso3s():
    """Input ISO3s that should throw an error."""
    return ["abcd", "ab", "123"]


def test_input_iso3(bad_iso3s):
    """Test that user input iso3 is validated, but can be uppercase."""
    with pytest.raises(ValueError):
        for iso3 in bad_iso3s:
            create_country_config(iso3=iso3)


def test_uppercase_input_iso3(mock_parse_yaml):
    """Test that uppercase iso3 converted to lowercase for filename."""
    fake_parser = mock_parse_yaml()
    create_country_config(iso3="ABC")
    _, args, _ = fake_parser.mock_calls[0]
    assert "abc.yaml" in str(args[0])


def test_config_iso3(mock_parse_yaml, bad_iso3s):
    """Test that iso3 in config file is validated."""
    mock_parse_yaml(output_list=[{"iso3": iso3} for iso3 in bad_iso3s])
    with pytest.raises(ValueError):
        for _ in bad_iso3s:
            create_country_config(iso3="abc")


def test_uppercase_config_iso3(mock_parse_yaml):
    """Test that uppercase iso3 in config file is converted to lowercase."""
    mock_parse_yaml(output_list=[{"iso3": "ABC"}])
    country_config = create_country_config(iso3="abc")
    assert country_config.iso3 == "abc"


def test_codab_validate_layer_base_name(mock_parse_yaml):
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


def test_codab_validate_admin_level_max(mock_parse_yaml):
    """Test that admin level can only be between 0 and 4."""
    config_base = {
        "iso3": "abc",
        "codab": {
            "hdx_dataset_name": "fake_dataset_name",
            "layer_base_name": "layer_base_name_{admin_level}",
            "admin_level_max": 1,
        },
    }
    config_correct = copy.deepcopy(config_base)
    config_incorrect_a = copy.deepcopy(config_base)
    config_incorrect_a["codab"]["admin_level_max"] = -1
    config_incorrect_b = copy.deepcopy(config_base)
    config_incorrect_b["codab"]["admin_level_max"] = 5
    mock_parse_yaml(
        output_list=[config_correct, config_incorrect_a, config_incorrect_b]
    )
    # Check that correct config runs without issue
    create_country_config(iso3="abc")
    # Check that incorrect config raises error for config incorrect a
    with pytest.raises(ValueError):
        create_country_config(iso3="abc")
    # Check that incorrect config raises error for config incorrect b
    with pytest.raises(ValueError):
        create_country_config(iso3="abc")


def test_fewsnet_validate_region_name(mock_parse_yaml):
    """Test that fewsnet requires correct region name."""
    config_base = {
        "iso3": "abc",
        "fewsnet": {"region_name": ""},
    }
    config_correct = copy.deepcopy(config_base)
    config_correct["fewsnet"]["region_name"] += "east-africa"
    config_incorrect = copy.deepcopy(config_base)
    config_incorrect["fewsnet"]["region_name"] += "fake_region_name"
    mock_parse_yaml(output_list=[config_correct, config_incorrect])
    # Check that correct config runs without issue
    country_config_valid = create_country_config(iso3="abc")
    assert country_config_valid.fewsnet == FewsNetConfig(
        region_name_code_mapping={
            "caribbean-central-america": "LAC",
            "central-asia": "CA",
            "east-africa": "EA",
            "southern-africa": "SA",
            "west-africa": "WA",
        },
        region_name="east-africa",
        region_code="EA",
    )
    # Check that config with incorrect region_name raises error
    with pytest.raises(ValueError):
        create_country_config(iso3="abc")
