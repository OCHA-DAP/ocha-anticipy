"""
FEWS NET processing.

Download and save the data provided by FEWS NET.
"""

import logging
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Union

from requests.exceptions import HTTPError

from aatoolbox.utils.io import download_url, unzip

logger = logging.getLogger(__name__)
BASE_URL_COUNTRY = (
    "https://fdw.fews.net/api/ipcpackage/"
    "?country_code={iso2}&collection_date={YYYY}-{MM}-01"
)
BASE_URL_REGION = (
    "https://fews.net/data_portal_download/download"
    "?data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
    "HFIC/{region_code}/{region_name}{YYYY}{MM}.zip"
)


def download_zip(
    url: str,
    zip_filename: str,
    output_dir: Path,
) -> bool:
    """
    Download and unzip the file at the url.

    Parameters
    ----------
    url : str
        url that contains the zip file to be downloaded
    zip_filename : str
        name of the zipfile
    output_dir : Path
        path of dir to which the zip content should be written

    Returns
    -------
    valid_file: bool
        if True, the url contains a valid zip file

    """
    valid_file = False

    try:
        # create tempdir to write zipfile to
        with TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / zip_filename
            download_url(url=url, save_path=zip_path)
            logger.info(f"Downloaded {url} to {zip_path}")

            try:
                unzip(zip_file_path=zip_path, save_dir=output_dir)
                logger.debug(f"Unzipped to {output_dir}")
                valid_file = True
            except zipfile.BadZipFile:
                # indicates that the url returned something that wasn't a
                # zip, happens often and indicates data for the given
                # country - date is not available
                logger.debug(
                    f"No zip data returned from url {url} "
                    f"check that the area and date exist."
                )

    except HTTPError as e:
        logger.info(e)

    return valid_file


def _download_fewsnet_country(
    date: datetime,
    iso2: str,
    output_dir: Path,
    use_cache: bool,
) -> bool:
    """
    Download fewsnet data that covers the iso2 country.

    Parameters
    ----------
    date : datetime
        date for which the data should be downloaded
        only the year and month part are used
        this commonly refers to the month of the Current Situation period
    iso2 : str
        iso2 code for which the data should be downloaded
    output_dir: Path
        path of dir to which the zip content should be written
    use_cache: bool
        if True, don't download if output_dir already exists

    Returns
    -------
    country_data : bool
        if True, country data for the given date and iso2 exists
    """
    url_country_date = BASE_URL_COUNTRY.format(
        iso2=iso2, YYYY=date.year, MM=date.month
    )

    output_dir_country = output_dir / f"{iso2}{date.strftime('%Y%m')}"
    zip_filename = f"{output_dir_country.name}.zip"
    if not output_dir_country.exists() or use_cache is False:
        country_data = download_zip(
            url=url_country_date,
            zip_filename=zip_filename,
            output_dir=output_dir_country,
        )
    else:
        country_data = True
    return country_data


def _download_fewsnet_region(
    date: datetime,
    region_name: str,
    region_code: str,
    output_dir: Path,
    use_cache: bool,
) -> bool:
    """
    Download fewsnet data that covers `region_name`.

    Parameters
    ----------
    date : datetime
        date for which the data should be downloaded
        only the year and month part are used
        this commonly refers to the month of the Current Situation period
    region_name : str
        name of the region to which the `iso2` belongs,
        e.g. "east-africa"
    region_code : str
        code that refers to the `region_name,
        e.g. "EA"
    output_dir : Path
        path to dir to which the data should be written
    use_cache : bool
        if True, don't download if output_dir already exists

    Returns
    -------
    region_data : bool
        if True, region data for the given date and region name exists
    """
    url_region_date = BASE_URL_REGION.format(
        region_code=region_code,
        region_name=region_name,
        YYYY=date.year,
        MM=date.month,
    )

    output_dir_region = output_dir / f"{region_code}{date.strftime('%Y%m')}"
    zip_filename_region = f"{output_dir_region.name}.zip"
    if not output_dir_region.exists() or use_cache is False:
        region_data = download_zip(
            url=url_region_date,
            zip_filename=zip_filename_region,
            output_dir=output_dir_region,
        )
    else:
        region_data = True
    return region_data


def download_fewsnet(
    date: datetime,
    iso2: str,
    region_name: str,
    region_code: str,
    output_dir: Union[Path, str],
    use_cache=True,
):
    """
    Retrieve the raw fewsnet data.

    Depending on the region and date, this data is published per region or
    per country. This function retrieves the country data
    if it exists, and else the regional data for `date` and `iso2`.

    Parameters
    ----------
    date : datetime
        date for which the data should be downloaded
        only the year and month part are used
        this commonly refers to the month of the Current Situation period
    iso2 : str
        iso2 code of the country of interest
    region_name : str
        name of the region to which the `iso2` belongs,
        e.g. "east-africa"
    region_code : str
        code that refers to the `region_name,
        e.g. "EA"
    output_dir : Path or str
        path to dir to which the data should be written
    use_cache : bool
        if True, don't download if output_dir already exists
    """
    # upper case iso2 and region_code since they are upper-cased
    # in fewsnet's url
    iso2 = iso2.upper()
    region_code = region_code.upper()
    # convert to path object if str
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    # we prefer the country data as this more nicely structured
    # thus first check if that is available
    country_data = _download_fewsnet_country(
        date=date, iso2=iso2, output_dir=output_dir, use_cache=use_cache
    )
    if not country_data:
        region_data = _download_fewsnet_region(
            date=date,
            region_name=region_name,
            region_code=region_code,
            output_dir=output_dir,
            use_cache=use_cache,
        )
        if not region_data:
            logger.info(f"No data found for {date.strftime('%Y-%m')}")
