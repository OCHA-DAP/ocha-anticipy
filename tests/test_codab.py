"""Test the COD AB data source."""
from pathlib import Path

import pytest
from conftest import FAKE_AA_DATA_DIR

from aatoolbox.datasources.codab import CodAB

ISO3 = "abc"


@pytest.fixture
def codab():
    """Fixture for CodAB mocked."""
    return CodAB(iso3=ISO3)


def test_codab_download(codab, mocker):
    """Test that COD AB download calls HDX API with correct params."""
    fake_downloader = mocker.patch(
        "aatoolbox.datasources.codab.get_dataset_from_hdx"
    )
    fake_hdx_address = "fake_hdx_address"
    fake_hdx_dataset_name = "fake_hdx_dataset_name"
    codab.download(
        hdx_address=fake_hdx_address, hdx_dataset_name=fake_hdx_dataset_name
    )
    _, call_args = fake_downloader.call_args
    assert call_args["hdx_address"] == fake_hdx_address
    assert call_args["hdx_dataset_name"] == fake_hdx_dataset_name
    assert (
        call_args["output_filepath"]
        == Path(FAKE_AA_DATA_DIR)
        / f"public/raw/{ISO3}/cod_ab/{ISO3}_cod_ab.shp.zip"
    )
