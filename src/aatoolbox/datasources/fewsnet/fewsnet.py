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
from typing import Optional

import geopandas as gpd
from hdx.location.country import Country
from typing_extensions import Literal

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

_VALID_PROJECTION_PERIODS_LITERAL = Literal["CS", "ML1", "ML2"]
_VALID_PROJECTION_PERIODS = ["CS", "ML1", "ML2"]


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
            # FN data can be regional, and thus we save the raw data
            # in "glb" instead of in the iso3 folder, even though part of it is
            # at country level
            is_global_raw=True,
            is_global_processed=False,
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
        self._region_code = self._country_config.fewsnet.region_code

    # mypy will give error Signature of "download" incompatible with supertype
    # "DataSource" due to `pub_year` and `pub_month` not being an arg in
    # `DataSource`. This is however valid so ignore mypy
    def download(  # type: ignore
        self,
        pub_year: int,
        pub_month: int,
        clobber: bool = False,
    ) -> Optional[Path]:
        """
        Retrieve the raw FEWS NET data.

        Depending on the region and date, this data is published per region or
        per country. This function retrieves the country data
        if it exists, and else the regional data for `pub_year`-`pub_month`.

        Parameters
        ----------
        pub_year: int
            publication year of the data that should be downloaded
        pub_month: int
            publication month of the data that should be downloaded. This
            commonly refers to the month of the Current Situation period
        clobber : bool, default = False
            If True, overwrites existing raw files

        Examples
        --------
        >>> from aatoolbox import create_country_config, FewsNet
        >>> # Download FEWS NET data for ETH published in 2021-06
        >>> country_config = create_country_config(iso3="eth")
        >>> fewsnet = FewsNet(country_config=country_config)
        >>> eth_fn_202106_path = fewsnet.download(pub_year=2021,pub_month=6)
        """
        self._check_date_validity(pub_year=pub_year, pub_month=pub_month)
        pub_month_str = f"{pub_month:02d}"
        # we prefer the country data as this more nicely structured
        # thus first check if that is available
        try:
            return self._download_country(
                pub_year=pub_year,
                pub_month=pub_month_str,
                clobber=clobber,
            )
        except ValueError:
            try:
                return self._download_region(
                    pub_year=pub_year,
                    pub_month=pub_month_str,
                    clobber=clobber,
                )
            except ValueError:
                raise RuntimeError(
                    "No country or regional data found for"
                    f" {pub_year}-{pub_month_str}"
                )

    def process(self, *args, **kwargs):
        """
        Process FEWS NET data.

        Method not implemented.
        """
        logger.info("`process()` method not yet implemented for FEWS NET.")

    def load(  # type: ignore
        self,
        pub_year: int,
        pub_month: int,
        projection_period: _VALID_PROJECTION_PERIODS_LITERAL,
    ) -> gpd.GeoDataFrame:
        """
        Load FEWS NET data.

        For the given `pub_year`, `pub_month` and `projection_period`.

        Parameters
        ----------
        pub_year: int
            publication year of the data that should be loaded
        pub_month: int
            publication month of the data that should be loaded. This
            refers to the first month of the Current Situation period
        projection_period: str
            The projection period to be loaded. This should be CS, ML1, or ML2.
            Referring to Current Situation, near term projection, and medium
            term projection respectively.

        Returns
        -------
        Geopandas DataFrame with the specified data.

        Examples
        --------
        >>> from aatoolbox import create_country_config, FewsNet
        >>> # Load FEWS NET data for ETH published in 2021-06 of medium-term
        ... projection period (ML1)
        >>> country_config = create_country_config(iso3="eth")
        >>> fewsnet = FewsNet(country_config=country_config)
        >>> gdf_eth_fn_202106 = fewsnet.load(pub_year=2021,pub_month=6,
        ... projection_period = "ML1")
        """
        if projection_period not in _VALID_PROJECTION_PERIODS:
            raise ValueError(
                "`projection_period` is not a valid projection_period. "
                f"It must be one of {', '.join(_VALID_PROJECTION_PERIODS)}"
            )

        self._check_date_validity(pub_year=pub_year, pub_month=pub_month)
        pub_month_str = f"{pub_month:02d}"
        dir_path = self._find_raw_dir_date(
            pub_year=pub_year, pub_month=pub_month_str
        )
        file_path = self._get_raw_file_projection_period(
            dir_path=dir_path, projection_period=projection_period
        )
        return gpd.read_file(file_path)

    def _check_date_validity(self, pub_year, pub_month):
        try:
            pub_date = datetime.datetime(year=pub_year, month=pub_month, day=1)
        except ValueError as err:
            raise ValueError(
                "The combination f pub_year-pub_month, {pub_year}-{pub_month},"
                " is not a valid date."
            ) from err
        if pub_date < datetime.datetime(year=2009, month=1, day=1):
            raise ValueError(
                "FEWSNET publishes data since 2009, so adjust your pub_year to"
                " be >=2009, currently {pub_year}"
            )
        elif pub_date > datetime.datetime.now():
            raise ValueError(
                "There is no data published in the future. The date should "
                "refer to the start month-year of the Current "
                "situation period."
            )

    def _download_country(
        self,
        pub_year: int,
        pub_month: str,
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
            iso2=self._iso2, YYYY=pub_year, MM=pub_month
        )

        output_dir_country = self._download(
            url=url_country_date,
            area=self._iso2,
            pub_year=pub_year,
            pub_month=pub_month,
        )
        return output_dir_country

    def _download_region(
        self,
        pub_year: int,
        pub_month: str,
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
            YYYY=pub_year,
            MM=pub_month,
        )

        output_dir_region = self._download(
            url=url_region_date,
            area=self._region_code,
            pub_year=pub_year,
            pub_month=pub_month,
        )
        return output_dir_region

    def _download(self, url, area, pub_year, pub_month):
        """
        Define output names and call _download_zip.

        url: str
            URL the zip file is located
        area: str
            Identifier of which area the data covers. This is either the ISO2
            or the region code
        pub_year: int
            publication year of the data that should be downloaded
        pub_month: int
            publication month of the data that should be downloaded. This
            commonly refers to the month of the Current Situation period
        """
        # filenames have upper iso2/regioncode, so use that for dirs as well
        output_dir = self._get_raw_dir_date(
            area=area, pub_year=pub_year, pub_month=pub_month
        )
        _download_zip(
            filepath=output_dir,
            zip_filename=self._get_zip_filename(
                area=area, pub_year=pub_year, pub_month=pub_month
            ),
            url=url,
        )
        return output_dir

    def _get_raw_dir_date(self, area, pub_year, pub_month):
        return self._raw_base_dir / f"{area}_{pub_year}{pub_month}"

    def _get_zip_filename(self, area, pub_year, pub_month):
        return f"{area}{pub_year}{pub_month}.zip"

    def _find_raw_dir_date(self, pub_year, pub_month):
        """
        Check if a dir exists for the given `pub_year`-`pub_month`.

        Should either cover the iso2 or region.
        If exists, returns the dir path.
        """
        country_dir = self._get_raw_dir_date(
            area=self._iso2, pub_year=pub_year, pub_month=pub_month
        )
        if country_dir.is_dir():
            return country_dir
        else:
            region_dir = self._get_raw_dir_date(
                area=self._region_code,
                pub_year=pub_year,
                pub_month=pub_month,
            )
            if region_dir.is_dir():
                return region_dir
            else:
                raise FileNotFoundError(
                    f"No data found for {pub_year}-{pub_month} covering "
                    "{self_country_configiso3} or {self._region_name}. "
                    "Please make sure the data exists and is downloaded"
                )

    def _get_raw_file_projection_period(self, dir_path, projection_period):
        file_path = dir_path / f"{dir_path.name}_{projection_period}.shp"
        if file_path.is_file():
            return file_path
        else:
            raise FileNotFoundError(
                f"File {file_path} not found. Make sure the projection "
                f"period {projection_period} exists for {dir_path.name}."
            )


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
            # country - year-month is not available

            raise ValueError(
                f"No zip data returned from url {url} "
                f"check that the area and year-month publication exist."
            )

    return filepath
