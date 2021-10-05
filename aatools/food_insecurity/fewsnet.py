from datetime import datetime
from pathlib import Path
import logging
from aatools.utils.io import download_url, unzip

logger = logging.getLogger(__name__)

def download_zip(
        url: str,
        zip_path: Path,
        output_dir: Path,
)-> bool:
    """
    Download the zip file at url, and unzip the file to the folder
    indicated by zip_path
    Parameters
    ----------
    url : str
        url that contains the zip file to be downloaded
    zip_path : Path
        path to which the content of the url should be downloaded
    output_dir : Path
        path of dir to which the zip content should be written

    Returns
    -------
    valid_file: bool
        if True, the url contains a valid zip file

    """
    valid_file = False

    try:
        download_url(url, zip_path)
        logger.info(
            f'Downloaded {url} to {zip_path}'
        )

        try:
            unzip(zip_path, output_dir)
            logger.debug(f"Unzipped {zip_path}")
            valid_file = True
        except Exception:
            # indicates that the url returned something that wasn't a
            # zip, happens often and indicates data for the given
            # country - date is not available
            logger.debug(
                f"No zip data returned from url {url}"
            )
        # remove the zip file
        zip_path.unlink()


    except Exception:
        logger.info(
            f"Couldn't download url {url}. "
            f"Check the url and output path: {zip_path}"
        )

    return valid_file

def download_fewsnet_country(
        date: datetime,
        iso2: str,
        output_dir: Path,
        #question: do you make this an optional arg
        #if the function is only called by another function
        #that already has it as optional arg?
        use_cache: bool,
)-> bool:
    """
    download fewsnet data that covers the country
    indicated by the iso2
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
    base_url_country = "https://fdw.fews.net/api/ipcpackage/"
    url_country = f"{base_url_country}?country_code={iso2}&collection_date={date.strftime('%Y-%m')}-01"
    output_dir_country = output_dir / f"{iso2}{date.strftime('%Y%m')}"
    #question: is there a better way to define this zip path?
    zip_path_country = Path(f"{output_dir_country}.zip")
    if not output_dir_country.exists() or use_cache==False:
        country_data = download_zip(url_country,zip_path_country,output_dir_country)
    else:
        country_data = True
    return country_data

def download_fewsnet_region(
        date: datetime,
        region_name: str,
        region_code: str,
        output_dir: str,
        use_cache: bool,
)-> bool:
    """
    download fewsnet data that covers the country
    indicated by `region_name`
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
    base_url_region = "https://fews.net/data_portal_download/download?data_file_path=http://shapefiles.fews.net.s3.amazonaws.com/HFIC/"
    url_region = (
        f"{base_url_region}{region_code}/{region_name}{date.strftime('%Y%m')}.zip"
    )
    zip_path_region = output_dir / f"{region_code}{date.strftime('%Y%m')}.zip"
    output_dir_region = output_dir / f"{region_code}{date.strftime('%Y%m')}"

    if not output_dir_region.exists() or use_cache == False:
        region_data = download_zip(url_region, zip_path_region, output_dir_region)
    else:
        region_data = True
    return region_data

def download_fewsnet(date: datetime,
                     iso2: str,
                     region_name:str,
                     region_code:str,
                     #question: is it good practice to have
                     # output_dir as input arg or define that with a config?
                     #question: is it too much to require it to be a Path object?
                     output_dir: Path,
                     use_cache=True):
    """
    Retrieve the raw fewsnet data.
    Depending on the region and date, this data is published per region or
    per country. This function retrieves the country data if it exists, and else
    the regional data for `date` and `iso2`.
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
    output_dir : Path
        path to dir to which the data should be written
    use_cache : bool
        if True, don't download if output_dir already exists
    """

    #question: is there a method to force the input to be upper-cased?
    #upper case iso2 and region_code since they are upper-cased
    #in fewsnet's url
    iso2=iso2.upper()
    region_code = region_code.upper()

    #we prefer the country data as this more nicely structured
    #thus first check if that is available
    # question: is it a good or bad idea to split this to two functions?
    #or better to put the content of those two functions directly here?
    country_data = download_fewsnet_country(date,iso2,output_dir,use_cache)
    if not country_data:
        region_data = download_fewsnet_region(date, region_name, region_code, output_dir, use_cache)
        if not region_data:
            logger.info(f"No data found for {date.strftime('%Y-%m')}")

