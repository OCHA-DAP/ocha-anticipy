"""
FEWS NET processing.

Download and save the data provided by FEWS NET as provided
on <https://fews.net/>.

FEWS NET is only available in a set of countries.
Check their website to see which countries are included.
"""

import logging
import zipfile
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional, Union

from hdx.location.country import Country

from aatoolbox.datasources.datasource import DataSource
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

# Only regions for which FewsNet provides data can be given as input
_VALID_REGION_NAMES = [
    "caribbean-central-america",
    "central-asia",
    "east-africa",
    "southern-africa",
    "west-africa",
]

_REGION_NAME_CODE_MAPPING = {
    "caribbean-central-america": "LAC",
    "central-asia": "CA",
    "east-africa": "EA",
    "southern-africa": "SA",
    "west-africa": "WA",
}

_MODULE_BASENAME = "fewsnet"


class FewsNet(DataSource):
    """
    Base class to retrieve FewsNet data.

    Parameters
    ----------
    country_config : CountryConfig
        Country configuration
    """

    def __init__(
        self,
        country_config,
    ):
        super().__init__(
            country_config=country_config,
            module_base_dir=_MODULE_BASENAME,
            is_public=True,
        )

        # FN data can be regional, and thus we save the raw data
        # in "glb" instead of in the iso3 folder, even though part of it is at
        # country level
        self._raw_base_dir = self._get_base_dir(
            is_public=True,
            is_raw=True,
            is_global=True,
        )

        self._iso2 = Country.get_iso2_from_iso3(self._country_config.iso3)
        if self._iso2 is None:
            logger.error("No ISO2 found for the given ISO3. Check your ISO3.")

    def download(
        self,
        date_pub: Union[date, str],
        clobber: bool = False,
    ) -> Path:
        """
        Retrieve the raw fewsnet data.

        Depending on the region and date, this data is published per region or
        per country. This function retrieves the country data
        if it exists, and else the regional data for `date_pub`.

        Parameters
        ----------
        date_pub : date or str
            date for which the data should be downloaded
            only the year and month part are used
            this commonly refers to the month of the Current Situation period
            if str, it should be in ISO 8601 format, e.g. '2021-10-01'
        clobber : bool, default = False
            If True, overwrites existing raw files

        Examples
        --------
        >>> download(date_pub="2020-10-01")
        """
        if (
            self._country_config.fewsnet.region_name  # type: ignore
            not in _VALID_REGION_NAMES
        ):
            raise ValueError(
                f"Invalid region name"  # type: ignore
                f" {self._country_config.fewsnet.region_name}"
            )

        # convert to datetime if str
        if not isinstance(date_pub, date):
            date_pub = datetime.strptime(date_pub, _DATE_FORMAT)

        # we prefer the country data as this more nicely structured
        # thus first check if that is available
        country_path = self._download_country(
            date_pub=date_pub,
            clobber=clobber,
        )
        if country_path is not None:
            return country_path
        else:
            region_path = self._download_region(
                date_pub=date_pub,
                clobber=clobber,
            )
        if region_path is not None:
            return region_path
        else:
            raise RuntimeError(
                f"No data found for {date_pub.strftime('%Y-%m')}"
            )

    def _download_country(
        self,
        date_pub: date,
        clobber: bool,
    ) -> Optional[Path]:
        """
        Download fewsnet data that covers the iso2 country.

        Returns
        -------
        country_data : str
            if data found return the output_dir, else return None
        """
        url_country_date = _BASE_URL_COUNTRY.format(
            iso2=self._iso2, YYYY=date_pub.year, MM=date_pub.month
        )

        # filenames have upper iso2, so use that for dirs as well
        output_dir_country = (
            self._raw_base_dir / f"{self._iso2}{date_pub.strftime('%Y%m')}"
        )
        zip_filename = f"{output_dir_country.name}.zip"
        if not output_dir_country.exists() or clobber is True:
            country_data = self._download_zip(
                url=url_country_date,
                zip_filename=zip_filename,
                output_dir=output_dir_country,
            )
        else:
            country_data = output_dir_country
        return country_data

    def _download_region(
        self,
        date_pub: date,
        clobber: bool,
    ) -> Optional[Path]:
        """
        Download fewsnet data that covers the region the iso3 belongs to.

        Returns
        -------
        region_data : str
            If region data exists, return the saved dir else return None
        """
        region_name = self._country_config.fewsnet.region_name  # type: ignore
        region_code = _REGION_NAME_CODE_MAPPING[region_name]
        url_region_date = _BASE_URL_REGION.format(
            region_code=region_code,
            region_name=region_name,
            YYYY=date_pub.year,
            MM=date_pub.month,
        )

        output_dir_region = (
            self._raw_base_dir / f""
            f"{region_code}"
            f"{date_pub.strftime('%Y%m')}"
        )
        zip_filename_region = f"{output_dir_region.name}.zip"
        if not output_dir_region.exists() or clobber is True:
            region_data = self._download_zip(
                url=url_region_date,
                zip_filename=zip_filename_region,
                output_dir=output_dir_region,
            )
        else:
            region_data = output_dir_region
        return region_data

    def _download_zip(
        self,
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
