"""Test COD AB methods as called from the pipeline."""
from pathlib import Path

import pytest

from aatoolbox.datasources.codab import MODULE_BASENAME
from aatoolbox.utils.io import parse_yaml
from tests.conftest import FAKE_AA_DATA_DIR
from tests.pipeline.conftest import CONFIG_FILE, ISO3


@pytest.fixture(autouse=True)
def downloader(mocker):
    """Mock the HDX download function."""
    return mocker.patch("aatoolbox.datasources.codab.get_dataset_from_hdx")


@pytest.fixture(autouse=True)
def gpd_read_file(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch("aatoolbox.datasources.codab.gpd.read_file")


def test_codab_download(pipeline_caller, downloader):
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


def test_codab_get_admin_level(pipeline_caller, gpd_read_file):
    """Test that get_codab retrieves expected file and layer name."""
    pipeline = pipeline_caller()
    admin_level = 2
    expected_layer_name = pipeline._config.codab.layer_base_name.format(
        admin_level=admin_level
    )

    pipeline.get_codab(admin_level=admin_level)

    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/{expected_layer_name}"
    )


def test_codab_too_high_admin_level(pipeline_caller):
    """Test raised error when too high admin level requested."""
    with pytest.raises(AttributeError):
        pipeline_caller().get_codab(admin_level=10)


def test_codab_custom(pipeline_caller, gpd_read_file):
    """Test that get_codab_custom retrieves expected file and layer name."""
    config_dict = parse_yaml(CONFIG_FILE)
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


def test_codab_custom_missing(pipeline_caller, gpd_read_file):
    """Test raised error when custom COD AB missing."""
    with pytest.raises(AttributeError):
        pipeline_caller().get_codab_custom(0)
