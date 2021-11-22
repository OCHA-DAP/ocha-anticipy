"""Test COD AB methods as called from the pipeline."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR, ISO3

from aatoolbox.datasources.codab.codab import _MODULE_BASENAME


@pytest.fixture(autouse=True)
def downloader(mocker):
    """Mock the HDX download function."""
    return mocker.patch(
        "aatoolbox.datasources.codab.codab.load_dataset_from_hdx"
    )


@pytest.fixture(autouse=True)
def gpd_read_file(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch("aatoolbox.datasources.codab.codab.gpd.read_file")


def test_codab_download(pipeline, downloader):
    """Test that load_codab calls the HDX API to download."""
    pipeline.load_codab(admin_level=2)
    downloader.assert_called_with(
        hdx_address=pipeline._config.codab.hdx_address,
        hdx_dataset_name=pipeline._config.codab.hdx_dataset_name,
        output_filepath=Path(FAKE_AA_DATA_DIR)
        / f"public/raw/{ISO3}/{_MODULE_BASENAME}/"
        f"{ISO3}_{_MODULE_BASENAME}.shp.zip",
    )


def test_codab_load_admin_level(pipeline, gpd_read_file):
    """Test that load_codab retrieves expected file and layer name."""
    admin_level = 2
    expected_layer_name = pipeline._config.codab.layer_base_name.format(
        admin_level=admin_level
    )

    pipeline.load_codab(admin_level=admin_level)

    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{_MODULE_BASENAME}/"
        f"{ISO3}_{_MODULE_BASENAME}.shp.zip/{expected_layer_name}"
    )


def test_codab_too_high_admin_level(pipeline):
    """Test raised error when too high admin level requested."""
    with pytest.raises(AttributeError):
        pipeline.load_codab(admin_level=10)


def test_codab_custom(pipeline, gpd_read_file):
    """Test that load_codab_custom retrieves expected file and layer name."""
    custom_layer_number = 1
    custom_layer_name_list = ["custom_layer_A", "custom_layer_B"]
    pipeline._config.codab.custom_layer_names = custom_layer_name_list
    pipeline.load_codab_custom(custom_layer_number)
    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{_MODULE_BASENAME}/"
        f"{ISO3}_{_MODULE_BASENAME}.shp.zip/"
        f"{custom_layer_name_list[custom_layer_number]}"
    )


def test_codab_custom_missing(pipeline, gpd_read_file):
    """Test raised error when custom COD AB missing."""
    with pytest.raises(AttributeError):
        pipeline.load_codab_custom(0)
