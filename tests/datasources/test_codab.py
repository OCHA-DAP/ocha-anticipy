"""Test COD AB methods."""
from pathlib import Path

import pytest

# TODO: this will break if there is another conftest
from conftest import FAKE_AA_DATA_DIR, ISO3

from aatoolbox import CodAB

MODULE_BASENAME = "cod_ab"


@pytest.fixture
def downloader(mocker):
    """Mock the HDX download function."""
    return mocker.patch(
        "aatoolbox.datasources.codab.codab.load_dataset_from_hdx"
    )


@pytest.fixture
def gpd_read_file(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch("aatoolbox.datasources.codab.codab.gpd.read_file")


def test_codab_download(mock_country_config, downloader):
    """Test that load_codab calls the HDX API to download."""
    codab = CodAB(country_config=mock_country_config)
    codab.download()
    downloader.assert_called_with(
        hdx_address=mock_country_config.codab.hdx_address,
        hdx_dataset_name=mock_country_config.codab.hdx_dataset_name,
        output_filepath=Path(FAKE_AA_DATA_DIR)
        / f"public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip",
    )


def test_codab_load_admin_level(mock_country_config, gpd_read_file):
    """Test that load_codab retrieves expected file and layer name."""
    codab = CodAB(country_config=mock_country_config)
    admin_level = 2
    expected_layer_name = mock_country_config.codab.layer_base_name.format(
        admin_level=admin_level
    )
    codab.load(admin_level=admin_level)

    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/{expected_layer_name}"
    )


def test_codab_too_high_admin_level(mock_country_config):
    """Test raised error when too high admin level requested."""
    codab = CodAB(country_config=mock_country_config)
    with pytest.raises(AttributeError):
        codab.load(admin_level=10)


def test_codab_custom(mock_country_config, gpd_read_file):
    """Test that load_codab_custom retrieves expected file and layer name."""
    custom_layer_number = 1
    custom_layer_name_list = ["custom_layer_A", "custom_layer_B"]
    mock_country_config.codab.custom_layer_names = custom_layer_name_list
    codab = CodAB(country_config=mock_country_config)
    codab.load_custom(custom_layer_number)
    gpd_read_file.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/"
        f"{custom_layer_name_list[custom_layer_number]}"
    )


def test_codab_custom_missing(mock_country_config, gpd_read_file):
    """Test raised error when custom COD AB missing."""
    codab = CodAB(country_config=mock_country_config)
    with pytest.raises(AttributeError):
        codab.load_custom(0)


def test_codab_load_fail(mock_country_config):
    """Test raises file not found error when load fails."""
    codab = CodAB(country_config=mock_country_config)
    # Remove file if it exists
    if codab._raw_filepath.exists():
        Path.unlink(codab._raw_filepath)
    with pytest.raises(FileNotFoundError) as excinfo:
        codab.load(admin_level=0)
    assert (
        "Make sure that you have already called the 'download' method"
        in str(excinfo.value)
    )
