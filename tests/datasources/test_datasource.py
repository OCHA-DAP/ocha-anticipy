"""Tests for the DataSource class."""
import pytest

from ochanticipy.datasources.datasource import DataSource


class DataSourceTesting(DataSource):
    """Class to inherit from DataSource to enable testing of DataSource."""

    def __init__(
        self,
        country_config,
        datasource_base_dir,
        is_public=False,
        is_global_raw=False,
        is_global_processed=False,
        config_datasource_name=None,
    ):
        super().__init__(
            country_config=country_config,
            datasource_base_dir=datasource_base_dir,
            is_public=is_public,
            is_global_raw=is_global_raw,
            is_global_processed=is_global_processed,
            config_datasource_name=config_datasource_name,
        )

    def download(self):
        """Download method required in DataSource class."""
        raise NotImplementedError

    def process(self):
        """Process method required in DataSource class."""
        raise NotImplementedError

    def load(self):
        """Load method required in DataSource class."""
        raise NotImplementedError


def test_config_attribute_name_validator(mock_country_config):
    """Test that correctly checked that datasource_name in the config."""
    # Test that attributes are passed to _datasource_config
    testclass_valid_attribute_name = DataSourceTesting(
        country_config=mock_country_config,
        datasource_base_dir="fake_dir_name",
        config_datasource_name="fewsnet",
    )
    assert (
        testclass_valid_attribute_name._datasource_config.region_name
        == "east-africa"
    )
    # Test that non-existant attribute throws error
    with pytest.raises(AttributeError):
        DataSourceTesting(
            country_config=mock_country_config,
            datasource_base_dir="fake_dir_name",
            config_datasource_name="fake_config_attribute",
        )
    # Test the None attribute throws error
    mock_country_config.fewsnet = None
    with pytest.raises(AttributeError):
        DataSourceTesting(
            country_config=mock_country_config,
            datasource_base_dir="fake_dir_name",
            config_datasource_name="fewsnet",
        )
