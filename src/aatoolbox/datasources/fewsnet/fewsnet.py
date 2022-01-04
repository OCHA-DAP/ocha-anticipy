"""
FEWS NET processing.

Download and save the data provided by FEWS NET as provided on .
"""

import logging
import zipfile
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Union

from typing_extensions import Literal

from aatoolbox.utils.io import download_url, unzip

logger = logging.getLogger(__name__)
_DATE_FORMAT = "%Y-%m-%d"
_BASE_URL_COUNTRY = (
    "https://fdw.fews.net/api/ipcpackage/"
    "?country_code={iso2}&collection_date={YYYY}-{MM}-01"
)
_BASE_URL_REGION = (
    "https://fews.net/data_portal_download/download"
    "?data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
    "HFIC/{region_code}/{region_name}{YYYY}{MM}.zip"
)

_VALID_REGION_NAMES = [
    "caribbean-central-america",
    "central-asia",
    "east-africa",
    "southern-africa",
    "west-africa",
]
_VALID_REGION_CODES = ["LAC", "EA", "CA", "SA", "WA"]


def download_fewsnet(
    date_pub: Union[date, str],
    iso2: str,
    region_name: Literal[
        "caribbean-central-america",
        "central-asia",
        "east-africa",
        "southern-africa",
        "west-africa",
    ],
    region_code: Literal["LAC", "EA", "CA", "SA", "WA"],
    output_dir: Union[Path, str],
    use_cache=True,
) -> Path:
    """
    Retrieve the raw fewsnet data.

    Depending on the region and date, this data is published per region or
    per country. This function retrieves the country data
    if it exists, and else the regional data for `date_pub` and `iso2`.

    Parameters
    ----------
    date_pub : date or str
        date for which the data should be downloaded
        only the year and month part are used
        this commonly refers to the month of the Current Situation period
        if str, it should be in ISO 8601 format, e.g. '2021-10-01'
    iso2 : str
        iso2 code of the country of interest.
        See https://fews.net/ for a list of countries that are
        included by FewsNet
    region_name : {'caribbean-central-america', 'central-asia',
        'east-africa', 'southern-africa', 'west-africa'}
        name of the region to which the `iso2` belongs.
        Only regions for which FewsNet provides data can be given as input
    region_code : {'LAC','EA','CA','SA','WA'}
        code that refers to the `region_name`
        These are defined by FewsNet
    output_dir : Path or str
        path to dir to which the data should be written
    use_cache : bool, default True
        if True, don't download if output_dir already exists

    Examples
    --------
    >>> download_fewsnet(date_pub="2020-10-01",iso2="et",
    ... region_code="east-africa", region_name="EA",
    ... output_dir="tmp",use_cache=False)
    """
    if region_name not in _VALID_REGION_NAMES:
        raise ValueError(f"Invalid region name {region_name}")
    if region_code not in _VALID_REGION_CODES:
        raise ValueError(f"Invalid region code {region_code}")

    # convert to datetime if str
    if not isinstance(date_pub, date):
        date_pub = datetime.strptime(date_pub, _DATE_FORMAT)
    # convert to path object if str
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)

    # we prefer the country data as this more nicely structured
    # thus first check if that is available
    country_path = _download_fewsnet_country(
        date_pub=date_pub,
        iso2=iso2,
        output_dir=output_dir,
        use_cache=use_cache,
    )
    if country_path is not None:
        return country_path
    else:
        region_path = _download_fewsnet_region(
            date_pub=date_pub,
            region_name=region_name,
            region_code=region_code,
            output_dir=output_dir,
            use_cache=use_cache,
        )
    if region_path is not None:
        return region_path
    else:
        raise RuntimeError(f"No data found for {date_pub.strftime('%Y-%m')}")


def _download_fewsnet_country(
    date_pub: date,
    iso2: str,
    output_dir: Path,
    use_cache: bool,
) -> Optional[Path]:
    """
    Download fewsnet data that covers the iso2 country.

    Parameters
    ----------
    date_pub : date
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
    country_data : str
        if data found return the output_dir, else return None
    """
    url_country_date = _BASE_URL_COUNTRY.format(
        iso2=iso2.upper(), YYYY=date_pub.year, MM=date_pub.month
    )

    # filenames have upper iso2, so use that for dirs as well
    output_dir_country = (
        output_dir / f"{iso2.upper()}{date_pub.strftime('%Y%m')}"
    )
    zip_filename = f"{output_dir_country.name}.zip"
    if not output_dir_country.exists() or use_cache is False:
        country_data = _download_zip(
            url=url_country_date,
            zip_filename=zip_filename,
            output_dir=output_dir_country,
        )
    else:
        country_data = output_dir_country
    return country_data


def _download_fewsnet_region(
    date_pub: date,
    region_name: str,
    region_code: str,
    output_dir: Path,
    use_cache: bool,
) -> Optional[Path]:
    """
    Download fewsnet data that covers `region_name`.

    Parameters
    ----------
    date_pub : date
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
    region_data : str
        If region data exists, return the saved dir else return None
    """
    url_region_date = _BASE_URL_REGION.format(
        region_code=region_code.upper(),
        region_name=region_name,
        YYYY=date_pub.year,
        MM=date_pub.month,
    )

    output_dir_region = (
        output_dir / f"{region_code.upper()}{date_pub.strftime('%Y%m')}"
    )
    zip_filename_region = f"{output_dir_region.name}.zip"
    if not output_dir_region.exists() or use_cache is False:
        region_data = _download_zip(
            url=url_region_date,
            zip_filename=zip_filename_region,
            output_dir=output_dir_region,
        )
    else:
        region_data = output_dir_region
    return region_data


def _download_zip(
    url: str,
    zip_filename: str,
    output_dir: Path,
) -> Optional[Path]:
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
    None if no valid file, else output_dir

    """
    # create tempdir to write zipfile to
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / zip_filename
        download_url(url=url, save_path=zip_path)
        logger.info(f"Downloaded {url} to {zip_path}")

        try:
            unzip(zip_file_path=zip_path, save_dir=output_dir)
            logger.debug(f"Unzipped to {output_dir}")

        except zipfile.BadZipFile:
            # indicates that the url returned something that wasn't a
            # zip, happens often and indicates data for the given
            # country - date is not available
            logger.debug(
                f"No zip data returned from url {url} "
                f"check that the area and date exist."
            )
            return None

    return output_dir
