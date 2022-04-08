"""
FEWS NET processing.

Download and save the data provided by FEWS NET as provided
on <https://fews.net/>.

FEWS NET is only available in a set of countries.
Check their website to see which countries are included.
"""

import datetime
import logging
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, Optional, get_args

import geopandas as gpd

# mypy gives an error Library stubs not installed for "dateutil.parser"
# known issue that is incorrect and doesn't make sense so ignore
from dateutil.parser import parse  # type: ignore
from hdx.location.country import Country

from aatoolbox.datasources.datasource import DataSource
from aatoolbox.utils.io import check_file_existence, download_url, unzip

logger = logging.getLogger(__name__)
_BASE_URL_COUNTRY = (
    "https://fdw.fews.net/api/ipcpackage/"
    "?country_code={iso2}&collection_date={YYYY}-{MM}-01"
)
_BASE_URL_REGION = (
    "https://fews.net/data_portal_download/download"
    "?data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/"
    "HFIC/{region_code}/{region_name}{YYYY}{MM}.zip"
)

# the URL uses a code, so map them
_REGION_NAME_CODE_MAPPING = {
    "caribbean-central-america": "LAC",
    "central-asia": "CA",
    "east-africa": "EA",
    "southern-africa": "SA",
    "west-africa": "WA",
}

_VALID_PROJECTION_PERIODS = Literal["CS", "ML1", "ML2"]


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
            datasource_base_dir="fewsnet",
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
            raise KeyError(
                "No ISO2 found for the given ISO3. Check your ISO3."
            )

        if self._country_config.fewsnet is None:
            raise KeyError(
                "The country configuration file does not contain"
                "any region name. Please update and try again."
            )

        self._region_name = self._country_config.fewsnet.region_name
        self._region_code = _REGION_NAME_CODE_MAPPING[self._region_name]

    # mypy will give error Signature of "download" incompatible with supertype
    # "DataSource" due `date_pub` not being an arg in `DataSource`. This is
    # however valid so ignore mypy
    def download(  # type: ignore
        self,
        date_pub: str,
        clobber: bool = False,
    ) -> Optional[Path]:
        """
        Retrieve the raw FEWS NET data.

        Depending on the region and date, this data is published per region or
        per country. This function retrieves the country data
        if it exists, and else the regional data for `date_pub`.

        Parameters
        ----------
        date_pub : str
            date for which the data should be downloaded
            only the year and month part are used
            this commonly refers to the month of the Current Situation period
        clobber : bool, default = False
            If True, overwrites existing raw files

        Examples
        --------
        >>> download(date_pub="2020-10-01")
        """
        date_pub_datetime = parse(date_pub)

        # we prefer the country data as this more nicely structured
        # thus first check if that is available
        try:
            return self._download_country(
                date_pub=date_pub_datetime,
                clobber=clobber,
            )
        except ValueError:
            try:
                return self._download_region(
                    date_pub=date_pub_datetime,
                    clobber=clobber,
                )
            except ValueError:
                raise RuntimeError(
                    "No country or regional data found for"
                    f" {date_pub_datetime.strftime('%Y-%m')}"
                )

    def process(self, *args, **kwargs):
        """
        Process FEWS NET data.

        Method not implemented.
        """
        logger.info("`process()` method not yet implemented for FEWS NET.")

    def load(  # type: ignore
        self,
        date_pub: str,
        projection_period: _VALID_PROJECTION_PERIODS,
    ) -> gpd.GeoDataFrame:
        """Load FEWS NET data for the given `date_pub`."""
        valid_projection_periods = get_args(_VALID_PROJECTION_PERIODS)
        if projection_period not in valid_projection_periods:
            raise ValueError(
                "`projection_period` is not a valid projection_period. "
                f"It must be one of {*valid_projection_periods,}"
            )
        date_pub_datetime = parse(date_pub)
        dir_path = self._find_raw_dir_date(date_pub=date_pub_datetime)
        file_path = self._get_raw_file_projection_period(
            dir_path=dir_path, projection_period=projection_period
        )
        return gpd.read_file(file_path)

    def _download_country(
        self,
        date_pub: datetime.date,
        clobber: bool,
    ) -> Optional[Path]:
        """
        Download fewsnet data that covers the iso2 country.

        Returns
        -------
        country_data : Path
            if data found return the output_dir, else return None
        """
        url_country_date = _BASE_URL_COUNTRY.format(
            iso2=self._iso2, YYYY=date_pub.year, MM=f"{date_pub.month:02d}"
        )

        output_dir_country = self._download(
            url=url_country_date, area=self._iso2, date_pub=date_pub
        )
        return output_dir_country

    def _download_region(
        self,
        date_pub: datetime.date,
        clobber: bool,
    ) -> Optional[Path]:
        """
        Download fewsnet data that covers the region the iso3 belongs to.

        Returns
        -------
        region_data : Path
            If region data exists, return the saved dir else return None
        """
        url_region_date = _BASE_URL_REGION.format(
            region_code=self._region_code,
            region_name=self._region_name,
            YYYY=date_pub.year,
            MM=f"{date_pub.month:02d}",
        )

        output_dir_region = self._download(
            url=url_region_date, area=self._region_code, date_pub=date_pub
        )
        return output_dir_region

    def _download(self, url, area, date_pub):
        """
        Define output names and call _download_zip.

        url: str
            URL the zip file is located
        area: str
            Identifier of which area the data covers. This is either the ISO2
            or the region code
        date_pub: datetime.date
            date for which the data should be downloaded
            only the year and month part are used
            this commonly refers to the month of the Current Situation period
        """
        # filenames have upper iso2/regioncode, so use that for dirs as well
        output_dir = self._get_raw_dir_date(area=area, date_pub=date_pub)
        _download_zip(
            filepath=output_dir,
            zip_filename=self._get_zip_filename(area=area, date_pub=date_pub),
            url=url,
        )
        return output_dir

    def _get_raw_dir_date(self, area, date_pub):
        return self._raw_base_dir / f"{area}_{date_pub.strftime('%Y%m')}"

    def _get_zip_filename(self, area, date_pub):
        return f"{area}{date_pub.strftime('%Y%m')}.zip"

    def _find_raw_dir_date(self, date_pub: datetime.date):
        """
        Check if a dir exists for the given `date_pub`.

        Should either cover the iso2 or region.
        If exists, returns the dir path.
        """
        try:
            dir_path = self._get_raw_dir_date(
                area=self._iso2, date_pub=date_pub
            )
            dir_path.resolve(strict=True)
            return dir_path
        except FileNotFoundError:
            try:
                dir_path = self._get_raw_dir_date(
                    area=self._region_code, date_pub=date_pub
                )
                dir_path.resolve(strict=True)
                return dir_path
            except FileNotFoundError as err:
                raise FileNotFoundError(
                    f"No data found for {date_pub} covering "
                    "{self_country_configiso3} or {self._region_name}. "
                    "Please make sure the data exists and is downloaded"
                ) from err

    def _get_raw_file_projection_period(self, dir_path, projection_period):
        try:
            file_path = dir_path / f"{dir_path.name}_{projection_period}.shp"
            file_path.resolve(strict=True)
            return file_path
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f"File {file_path} not found. Make sure the projection "
                f"period {projection_period} exists for {dir_path.name}."
            ) from err


@check_file_existence
def _download_zip(
    filepath: Path,
    zip_filename: str,
    url: str,
) -> Optional[Path]:
    """
    Download and unzip the file at the url.

    Parameters
    ----------
    zip_filename : str
        name of the zipfile
    url : str
        url that contains the zip file to be downloaded

    Returns
    -------
    output_dir : Path
        None if no valid file, else output_dir

    """
    # create tempdir to write zipfile to
    with TemporaryDirectory() as temp_dir:
        zip_path = Path(temp_dir) / zip_filename
        download_url(url=url, save_path=zip_path)
        logger.info(f"Downloaded {url} to {zip_path}")

        try:
            unzip(zip_file_path=zip_path, save_dir=filepath)
            logger.debug(f"Unzipped to {filepath}")

        except zipfile.BadZipFile:
            # indicates that the url returned something that wasn't a
            # zip, happens often and indicates data for the given
            # country - date is not available

            raise ValueError(
                f"No zip data returned from url {url} "
                f"check that the area and date exist."
            )

    return filepath
