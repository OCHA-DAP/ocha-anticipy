"""Tests for the FewsNet module."""

from pathlib import Path

import pytest

from aatoolbox.datasources.fewsnet.fewsnet import download_fewsnet

# do we also want feature tests?


# I think there should be a neater way to do this with
# indirect parameterization, but dont fully understand
# https://stackoverflow.com/questions/18011902/pass-a-parameter-to-a-fixture-function
@pytest.fixture
def mock_download_call(mocker):
    """
    Run download_fewsnet with mocker objects and return the url.

    Parameters
    ----------
    mocker: hmm not sure what to say here
    also can kind of input the `country_data` argument, should
    that go here as well?

    Returns
    -------
    url
    """
    FakeDownloadUrl = mocker.patch(
        "aatoolbox.datasources.fewsnet.fewsnet.download_url"
    )
    mocker.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
    # !! Only working if using an output dir that already exists..
    # this is because we are mocking, when running the
    # function normally it works
    output_dir = Path("bla")

    def _method(country_data):
        if not country_data:
            mocker.patch(
                "aatoolbox.datasources.fewsnet.fewsnet."
                "_download_fewsnet_country",
                return_value=False,
            )
        download_fewsnet(
            date="2020-10-01",
            iso2="et",
            region_name="east-africa",
            region_code="EA",
            output_dir=output_dir,
            use_cache=False,
        )

        _, kwargs_download_url = FakeDownloadUrl.call_args

        return kwargs_download_url["url"]

    return _method


def test_download_fewsnet_country(mock_download_call):
    """
    Test that the correct country url is returned.

    Parameters
    ----------
    mock_download_call
    """
    url = mock_download_call(True)

    assert (
        url == "https://fdw.fews.net/api/ipcpackage/"
        "?country_code=ET&collection_date=2020-10-01"
    )


def test_download_fewsnet_region(mock_download_call):
    """
    Test that the correct region url is returned.

    Parameters
    ----------
    mock_download_call
    """
    url = mock_download_call(False)

    assert (
        url == "https://fews.net/data_portal_download/download?"
        "data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
        "HFIC/EA/east-africa202010.zip"
    )


# should we also mock if there is no country nor regional data?


# Old stuff
# def test_download_fewsnet_country(mocker):
#     FakeDownloadUrl = mocker.patch(
#         "aatoolbox.datasources.fewsnet.fewsnet.download_url"
#     )
#     mocker.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
#     output_dir = Path("bla")
#     download_fewsnet(
#         date="2020-10-01",
#         iso2="et",
#         region_name="east-africa",
#         region_code="EA",
#         output_dir=output_dir,
#         use_cache=False,
#     )
#     _, kwargs_download_url = FakeDownloadUrl.call_args
#     print(kwargs_download_url)
#     assert (
#         kwargs_download_url["url"] == "https://fdw.fews.net/api/ipcpackage/"
#         "?country_code=ET&collection_date=2020-10-01"
#     )
#
#
# def test_download_fewsnet_region(mocker):
#     mocker.patch(
#         "aatoolbox.datasources.fewsnet.fewsnet._download_fewsnet_country",
#         return_value=False,
#     )
#     FakeDownloadUrl = mocker.patch(
#         "aatoolbox.datasources.fewsnet.fewsnet.download_url"
#     )
#     mocker.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
#     #!! Only working if using an output dir that already exists..
#     # this is because we are mocking,
#     # when running the function normally it works
#     output_dir = Path("bla")
#     download_fewsnet(
#         date="2020-10-01",
#         iso2="et",
#         region_name="east-africa",
#         region_code="EA",
#         output_dir=output_dir,
#         use_cache=False,
#     )
#     _, kwargs_download_url = FakeDownloadUrl.call_args
#     assert (
#         kwargs_download_url["url"]
#         == "https://fews.net/data_portal_download/download?data_file_path"
#         "=http://shapefiles.fews.net.s3.amazonaws.com/"
#         "HFIC/EA/east-africa202010.zip"
#     )


#
#
# @mock.patch("aatoolbox.datasources.fewsnet.fewsnet.download_url")
# @mock.patch("aatoolbox.datasources.fewsnet.fewsnet.unzip")
# def test_download_country(FakeUnzip, FakeDownloadUrl):
#     """
#     Test to check the call to _download_fewsnet_country().
#
#     :param FakeUnzip: Mock object for unzipping
#     :param FakeDownloadUrl: Mock object for downloading url
#     """
#     output_dir = Path("tmp")
#     _download_fewsnet_country(
#         date=datetime(year=2020,month=10,day=1),
#         iso2="et",
#         output_dir=output_dir,
#         use_cache=False,
#     )
#     _, kwargs_download_url = FakeDownloadUrl.call_args
#     assert (
#         kwargs_download_url["url"] == "https://fdw.fews.net/api/ipcpackage/"
#         "?country_code=ET&collection_date=2020-10-01"
#     )
#
#     #what is there to test with the unzip?..
#     # _, kwargs_unzip = FakeUnzip.call_args
#     # print(kwargs_unzip)
#     #
#     # jkf
#
