"""Test COD AB methods as called from the pipeline."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR

from aatoolbox.datasources.codab import MODULE_BASENAME
from aatoolbox.pipeline import Pipeline
from aatoolbox.utils.io import parse_yaml

ISO3 = "abc"
FAKE_CONFIG_FILE = "tests/fake_config.yaml"


@pytest.fixture
def pipeline_caller(mocker):
    """Fixture for pipeline with test config params."""

    def _pipeline_caller(config_dict: dict = None):
        if config_dict is None:
            config_dict = parse_yaml(FAKE_CONFIG_FILE)
        mocker.patch(
            "aatoolbox.config.countryconfig.parse_yaml",
            return_value=config_dict,
        )
        return Pipeline(iso3_unvalidated=ISO3)

    return _pipeline_caller


@pytest.fixture
def downloader(mocker):
    """Mock the HDX download function."""
    return mocker.patch("aatoolbox.datasources.codab.get_dataset_from_hdx")


@pytest.fixture
def gpd_read_file(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch("aatoolbox.datasources.codab.gpd.read_file")


def test_codab_download(pipeline_caller, downloader, gpd_read_file):
    """Test that get_codab calls the HDX API to download."""
    pipeline = pipeline_caller()
    pipeline.get_codab(admin_level=2)
    downloader.assert_called_with(
        hdx_address=pipeline._config.codab.hdx_address,
        hdx_dataset_name=pipeline._config.codab.hdx_dataset_name,
        output_filepath=Path(FAKE_AA_DATA_DIR)
        / f"public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip",
    )


def test_codab_get_admin_level(pipeline_caller, downloader, gpd_read_file):
    """Test that get_codab retrieves expected file and layer name."""
    pipeline = pipeline_caller()
    admin_level = 2
    expected_layer_name = pipeline._config.codab.layer_base_name.format(
        admin_level=admin_level
    )

    pipeline_caller().get_codab(admin_level=admin_level)

    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/{expected_layer_name}"
    )


def test_codab_custom(pipeline_caller, downloader, gpd_read_file):
    """Test that get_codab_custom retrieves expected file and layer name."""
    config_dict = parse_yaml(FAKE_CONFIG_FILE)
    custom_layer_name_list = ["custom_layer_A", "custom_layer_B"]
    config_dict["codab"]["custom_layer_names"] = custom_layer_name_list
    custom_layer_number = 1

    pipeline_caller(config_dict=config_dict).get_codab_custom(
        custom_layer_number
    )
    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/"
        f"{custom_layer_name_list[custom_layer_number]}"
    )
