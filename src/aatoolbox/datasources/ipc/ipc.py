"""
Download the data provided by IPC in the tracking tool.

Link: http://www.ipcinfo.org/ipc-country-analysis/population-tracking-tool
IPC also has a mapping tool from which you can download a geojson
but the URL for that is not accessible and not all analyses are published on it
therefore this code uses the excel data provided by the tracking tool
The downloaded data has an "Area" column. It depends on the country which
level this is reported at.
Commonly it is admin2, but can also be at livelihood zone
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from requests import HTTPError

from aatoolbox.utils.io import download_url

logger = logging.getLogger(__name__)

# earliest available data can be 2017, so use that in URL
IPC_URL = (
    "http://mapipcissprd.us-east-1.elasticbeanstalk.com/api/"
    "public/population-tracking-tool/data/2017,{max_year}/"
    "?export=true&condition=A&country={iso2}"
)

# Analysis name, Country Population, % of total county Pop
# are not being used in our current analysis so not mapping them
IPC_COLUMN_NAME_MAPPING = {
    "Country": "ADMIN0",
    "Level 1 Name": "ADMIN1",
    "Area": "area",
    "Area ID": "area_id",
    "Date of Analysis": "date",
    "#": "reported_pop_CS",
    "Area Phase": "CS_phase",
    "Analysis Period": "CS_val",
    "#.1": "CS_1",
    "%": "CS_1_perc",
    "#.2": "CS_2",
    "%.1": "CS_2_perc",
    "#.3": "CS_3",
    "%.2": "CS_3_perc",
    "#.4": "CS_4",
    "%.3": "CS_4_perc",
    "#.5": "CS_5",
    "%.4": "CS_5_perc",
    "#.6": "CS_3p",
    "%.5": "CS_3p_perc",
    "#.7": "reported_pop_ML1",
    "Area Phase.1": "ML1_phase",
    "Analysis Period.1": "ML1_val",
    "#.8": "ML1_1",
    "%.6": "ML1_1_perc",
    "#.9": "ML1_2",
    "%.7": "ML1_2_perc",
    "#.10": "ML1_3",
    "%.8": "ML1_3_perc",
    "#.11": "ML1_4",
    "%.9": "ML1_4_perc",
    "#.12": "ML1_5",
    "%.10": "ML1_5_perc",
    "#.13": "ML1_3p",
    "%.11": "ML1_3p_perc",
    "#.14": "reported_pop_ML2",
    "Area Phase.2": "ML2_phase",
    "Analysis Period.2": "ML2_val",
    "#.15": "ML2_1",
    "%.12": "ML2_1_perc",
    "#.16": "ML2_2",
    "%.13": "ML2_2_perc",
    "#.17": "ML2_3",
    "%.14": "ML2_3_perc",
    "#.18": "ML2_4",
    "%.15": "ML2_4_perc",
    "#.19": "ML2_5",
    "%.16": "ML2_5_perc",
    "#.20": "ML2_3p",
    "%.17": "ML2_3p_perc",
}


def _preprocess_raw_data(
    raw_file_path: Path,
    output_path: Path,
):
    """
    Preprocess the downloaded data by checking.

    if the file contains data and changing column names
    :param raw_file_path: path to the file with the data to read
    :param output_path: path to write the preprocessed file to
    """
    df = pd.read_excel(
        raw_file_path,
        # on previous rows there is logo etc
        header=[11],
    )

    # only select rows with data, i.e. have a date of analysis
    # often have some rows with unneeded info for analysis
    # e.g. the disclaimer
    df = df[df["Date of Analysis"].notnull()]
    if df.empty:
        # a file from the url is downloaded regardless of whether
        # the iso2 is corrector ipc has done any analyses
        # thus check if there is actual data
        # we might want to check for correctness of iso2 already
        # before downloading (or when setting-up the config)
        raise RuntimeError(
            "Downloaded file doesn't contain any data. "
            "Make sure your iso2 code is correct."
        )

    # ipc excel file comes with horrible column names, so change them to
    # better understandable ones
    df = df.rename(columns=IPC_COLUMN_NAME_MAPPING)
    # due to excel settings, the percentages are on the 0 to 1 scale so
    # change to 0-100
    perc_cols = [c for c in df.columns if "perc" in c]
    df[perc_cols] = df[perc_cols] * 100
    df["date"] = pd.to_datetime(df["date"])

    # write preprocessed data to file
    df.to_csv(output_path, index=False)


def download_ipc(iso3: str, iso2: str, output_dir: Union[Path, str]):
    """
    Retrieve the IPC data from their Population Tracking Tool.

    Also do some preprocessing and save the outputs
    Since the data size per country is very small, we always
    download all data for the given country
    :param iso3: iso3 code of country of interest
    :param iso2: iso2 code of country of interest
    :param output_dir: path to directory the file should be saved to

    Examples
    --------
    >>> download_fewsnet(iso3="som",iso2="so,output_dir=output_dir)
    """
    # convert to path object if str
    if isinstance(output_dir, str):
        output_dir = Path(output_dir)
    # last year to retrieve data for
    # URL also works if this is in the future
    max_year = datetime.now().year
    url = IPC_URL.format(
        max_year=max_year,
        iso2=iso2,
    )
    raw_output_path = output_dir / f"{iso3}_ipc_raw.xlsx"

    # have one file with all data per country, so also download if file already
    # exists to make sure it contains the newest data (contrary to
    # fewsnet)
    try:
        download_url(url, raw_output_path)
    except HTTPError:
        raise RuntimeError(f"Cannot download IPC data for {iso3} from {url}")

    preprocessed_output_path = output_dir / f"{iso3}_ipc_preprocessed.csv"
    _preprocess_raw_data(raw_output_path, preprocessed_output_path)
