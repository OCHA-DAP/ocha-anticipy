"""Test the COD AB data source."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR

from aatoolbox.datasources.codab import MODULE_BASENAME, CodAB

ISO3 = "abc"


@pytest.fixture
def codab():
    """Fixture for CodAB mocked."""
    return CodAB(iso3=ISO3)


def test_download(codab, mocker):
    """Test that COD AB download calls HDX API with correct params."""
    fake_downloader = mocker.patch(
        "aatoolbox.datasources.codab.get_dataset_from_hdx"
    )
    fake_hdx_address = "fake_hdx_address"
    fake_hdx_dataset_name = "fake_hdx_dataset_name"
    codab.download(
        hdx_address=fake_hdx_address, hdx_dataset_name=fake_hdx_dataset_name
    )
    _, call_kwargs = fake_downloader.call_args
    fake_downloader.assert_called_with(
        hdx_address=fake_hdx_address,
        hdx_dataset_name=fake_hdx_dataset_name,
        output_filepath=Path(FAKE_AA_DATA_DIR)
        / f"public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip",
    )


def test_get_admin_layer(codab, mocker):
    """Test that retrieving the admin layer calls the correct layer name."""
    fake_gpd = mocker.patch("aatoolbox.datasources.codab.gpd.read_file")
    fake_layer_name = "fake_layer_name.shp"
    codab.get_admin_layer(layer_name=fake_layer_name)
    fake_gpd.assert_called_with(
        f"zip:///{FAKE_AA_DATA_DIR}/public/raw/{ISO3}/{MODULE_BASENAME}/"
        f"{ISO3}_{MODULE_BASENAME}.shp.zip/{fake_layer_name}"
    )
