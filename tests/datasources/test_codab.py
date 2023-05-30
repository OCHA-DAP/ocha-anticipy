"""Test COD AB methods."""
from unittest.mock import call

import pytest

from ochanticipy import CodAB, create_custom_country_config

DATASOURCE_BASE_DIR = "cod_ab"
MULTI_RESOURCE_CONFIG = "tests/datasources/fake_codab_multi.yaml"


@pytest.fixture
def mock_config_multi():
    """Fixture for CODAB with multiple resources."""
    return create_custom_country_config(filepath=MULTI_RESOURCE_CONFIG)


@pytest.fixture
def downloader(mocker):
    """Mock the HDX download function."""
    return mocker.patch(
        "ochanticipy.datasources.codab.codab.load_resource_from_hdx"
    )


@pytest.fixture
def gpd_read_file(mocker):
    """Mock GeoPandas file reading function."""
    return mocker.patch("ochanticipy.datasources.codab.codab.gpd.read_file")


def test_codab_download(mock_aa_data_dir, mock_country_config, downloader):
    """Test that load_codab calls the HDX API to download."""
    codab = CodAB(country_config=mock_country_config)
    codab.download()
    downloader.assert_called_with(
        hdx_dataset=f"cod-ab-{mock_country_config.iso3}",
        hdx_resource_name=mock_country_config.codab.hdx_resource_name,
        output_filepath=mock_aa_data_dir
        / f"public/raw/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}_adm.shp.zip",
    )


def test_codab_download_multi(mock_aa_data_dir, mock_config_multi, downloader):
    """Test that download calls the HDX API to download for multi resources."""
    codab = CodAB(country_config=mock_config_multi)
    codab.download()
    downloader.assert_has_calls(
        [
            call(
                hdx_dataset=f"cod-ab-{mock_config_multi.iso3}",
                hdx_resource_name=mock_config_multi.codab.hdx_resource_name[i],
                output_filepath=mock_aa_data_dir
                / f"public/raw/{mock_config_multi.iso3}/"
                f"{DATASOURCE_BASE_DIR}/{mock_config_multi.iso3}_"
                f"adm{i}.shp.zip",
            )
            for i in range(4)
        ]
    )


def test_codab_load_admin_level(
    mock_aa_data_dir, mock_country_config, gpd_read_file
):
    """Test that load_codab retrieves expected file and layer name."""
    codab = CodAB(country_config=mock_country_config)

    # First checking layer_base_name
    expected_layer_name = "fake_layer_base_name_level1"
    codab.load(admin_level=1)

    gpd_read_file.assert_called_with(
        f"zip://{mock_aa_data_dir}/public/raw/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}_"
        f"adm.shp.zip/{expected_layer_name}"
    )

    # Then checking custom name
    expected_layer_name = "admin2_custom_name"
    codab.load(admin_level=2)

    gpd_read_file.assert_called_with(
        f"zip://{mock_aa_data_dir}/public/raw/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}_"
        f"adm.shp.zip/{expected_layer_name}"
    )


def test_codab_multi_load_admin_level(
    mock_aa_data_dir, mock_config_multi, gpd_read_file
):
    """Test that load_codab retrieves expected file and layer name."""
    codab = CodAB(country_config=mock_config_multi)

    # First checking layer_base_name
    expected_layer_name = "fake_layer_base_name_level1"
    codab.load(admin_level=1)

    gpd_read_file.assert_called_with(
        f"zip://{mock_aa_data_dir}/public/raw/{mock_config_multi.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_config_multi.iso3}_"
        f"adm1.shp.zip/{expected_layer_name}"
    )


def test_codab_too_high_admin_level(mock_country_config):
    """Test raised error when too high admin level requested."""
    codab = CodAB(country_config=mock_country_config)
    with pytest.raises(AttributeError):
        codab.load(admin_level=10)


def test_codab_custom(mock_aa_data_dir, mock_country_config, gpd_read_file):
    """Test that load_codab_custom retrieves expected file and layer name."""
    custom_layer_number = 1
    custom_layer_name_list = ["custom_layer_A", "custom_layer_B"]

    mock_country_config.codab.custom_layer_names = custom_layer_name_list
    codab = CodAB(country_config=mock_country_config)
    codab.load_custom(custom_layer_number)
    gpd_read_file.assert_called_with(
        f"zip://{mock_aa_data_dir}/public/raw/{mock_country_config.iso3}/"
        f"{DATASOURCE_BASE_DIR}/{mock_country_config.iso3}_"
        f"adm.shp.zip/"
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
    # Remove file for admin0 if it exists
    codab._raw_filepaths[0].unlink(missing_ok=True)
    with pytest.raises(FileNotFoundError) as excinfo:
        codab.load(admin_level=0)
    assert (
        "Make sure that you have already called the 'download' method"
        in str(excinfo.value)
    )
