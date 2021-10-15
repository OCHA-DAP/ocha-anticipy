"""Use HDX python API to download data."""
import logging
from pathlib import Path

from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration

USER_AGENT = "aa-toolbox"

logger = logging.getLogger(__name__)
Configuration.create(user_agent=USER_AGENT, hdx_read_only=True)


def get_dataset_from_hdx(
    hdx_address: str, hdx_dataset_name: str, output_directory: Path
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
    output_directory : Path
        Target directory for the dataset

    Returns
    -------
    The full path of the downloaded dataset

    """
    logger.info(f"Querying HDX API for dataset {hdx_address}")
    resources = Dataset.read_from_hdx(hdx_address).get_resources()
    for resource in resources:
        if resource["name"] == hdx_dataset_name:
            logger.info(f"Downloading dataset {hdx_dataset_name}")
            _, output_filename = resource.download(folder=output_directory)
            logger.info(f"Saved to {output_filename}")
            return Path(output_filename)
    raise FileNotFoundError(
        f'HDX dataset with address "{hdx_address}" and name '
        f'"{hdx_dataset_name}" not found'
    )
