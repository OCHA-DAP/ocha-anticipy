"""Tests for the FewsNet module."""

from datetime import date
from pathlib import Path
from unittest import mock

from aatoolbox.datasources.fewsnet.fewsnet import _download_fewsnet_country


@mock.patch("aatoolbox.datasources.fewsnet.fewsnet.download_url")
@mock.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
def test_download_country(FakeUnzip, FakeDownloadUrl):
    """
    Test to check the call to _download_fewsnet_country().

    :param FakeUnzip: Mock object for unzipping
    :param FakeDownloadUrl: Mock object for downloading url
    """
    output_dir = Path("bla")
    _download_fewsnet_country(
        date=date(year=2020, month=10, day=1),
        iso2="et",
        output_dir=output_dir,
        use_cache=False,
    )
    _, kwargs_download_url = FakeDownloadUrl.call_args
    assert (
        kwargs_download_url["url"] == "https://fdw.fews.net/api/ipcpackage/"
        "?country_code=ET&collection_date=2020-10-01"
    )
