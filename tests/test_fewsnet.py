"""Tests for the FewsNet module."""
import pytest

from aatoolbox.datasources.fewsnet.fewsnet import download_fewsnet


@pytest.fixture
def _mock_download_call(mocker, tmp_path):
    """
    Call the download_fewsnet function.

    Use mocked url and unzip call and have option to set output
    of country and region download functions
    """
    FakeDownloadUrl = mocker.patch(
        "aatoolbox.datasources.fewsnet.fewsnet.download_url"
    )
    mocker.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")

    # this enables us to set the output of the _download_fewsnet_country()
    # and _download_fewsnet_region functions
    # the country_data and region_data arguments can be passed
    # as arg when calling _mock_download_call
    def _method(country_data, region_data):
        if not country_data:
            mocker.patch(
                "aatoolbox.datasources.fewsnet.fewsnet."
                "_download_fewsnet_country",
                return_value=None,
            )
        if not region_data:
            mocker.patch(
                "aatoolbox.datasources.fewsnet.fewsnet."
                "_download_fewsnet_region",
                return_value=None,
            )
        output_path = download_fewsnet(
            date_pub="2020-10-01",
            iso2="et",
            region_name="east-africa",
            region_code="EA",
            output_dir=tmp_path,
            use_cache=False,
        )

        _, kwargs_download_url = FakeDownloadUrl.call_args

        return kwargs_download_url["url"], output_path, tmp_path

    return _method


def test_download_fewsnet_country(_mock_download_call):
    """Test that the correct country url and path is returned."""
    url, output_path, tmp_path = _mock_download_call(
        country_data=True, region_data=True
    )

    assert (
        url == "https://fdw.fews.net/api/ipcpackage/"
        "?country_code=ET&collection_date=2020-10-01"
    )

    assert output_path == tmp_path / "ET202010"


def test_download_fewsnet_region(_mock_download_call):
    """Test that the correct region url is returned."""
    url, output_path, tmp_path = _mock_download_call(
        country_data=False, region_data=True
    )

    assert (
        url == "https://fews.net/data_portal_download/download?"
        "data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
        "HFIC/EA/east-africa202010.zip"
    )

    assert output_path == tmp_path / "EA202010"


def test_download_fewsnet_nodata(_mock_download_call):
    """Test that RuntimeError is returned when no data exists."""
    with pytest.raises(RuntimeError) as e:
        url, output_path, tmp_path = _mock_download_call(
            country_data=False, region_data=False
        )
        assert output_path is None
    assert "No data found for 2020-10" in str(e.value)
