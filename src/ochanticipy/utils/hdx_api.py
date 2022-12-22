"""Use HDX python API to download data."""
import logging
import shutil
import tempfile
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

USER_AGENT = "ocha-anticipy"

logger = logging.getLogger(__name__)
Configuration.create(
    hdx_site="prod", user_agent=USER_AGENT, hdx_read_only=True
)


def load_dataset_from_hdx(
    hdx_address: str, hdx_dataset_name: str, output_filepath: Path
) -> Path:
    """
    Use the HDX API to download a dataset based on the address and dataset ID.

    Parameters
    ----------
    hdx_address : str
        The page where the dataset resides on on HDX. Can be found
        by taking the portion of the url after ``data.humdata.org/dataset/``
    hdx_dataset_name : str
        Dataset name on HDX. Can be found by taking the filename as it
        appears on the dataset page.
    output_filepath : Path
        Target filepath for the dataset

    Returns
    -------
    The full path of the downloaded dataset

    """
    logger.info(f"Querying HDX API for dataset {hdx_address}")
    resources = Dataset.read_from_hdx(hdx_address).get_resources()
    logger.debug(f"Found the following resources: {resources}")
    for resource in resources:
        if resource["name"] == hdx_dataset_name:
            logger.info(f"Downloading dataset {hdx_dataset_name}")
            with tempfile.TemporaryDirectory() as tempdir:
                _, downloaded_filepath = resource.download(folder=tempdir)
                output_filepath.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(downloaded_filepath, output_filepath)
            logger.info(f"Saved to {output_filepath}")
            return Path(output_filepath)
    raise FileNotFoundError(
        f'Dataset with name "{hdx_dataset_name}" not found'
        f'at HDX address "{hdx_address}".'
    )
