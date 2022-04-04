"""Fixtures for all pipeline-related tests."""
import pytest

from aatoolbox import create_country_config
from aatoolbox.utils.io import parse_yaml

CONFIG_FILE = "tests/datasources/fake_config.yaml"
ISO3 = "abc"
ISO2 = "ab"


@pytest.fixture(autouse=True)
def mock_aa_data_dir(tmp_path_factory, mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    mock_aa_data_dir_path = tmp_path_factory.mktemp(
        basename="test_aa_data_dir"
    )
    mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": str(mock_aa_data_dir_path)},
    )
    return mock_aa_data_dir_path


@pytest.fixture
def mock_country_config(mocker):
    """Fixture for pipeline with test config params."""
    mocker.patch(
        "aatoolbox.config.countryconfig.parse_yaml",
        return_value=parse_yaml(CONFIG_FILE),
    )
    return create_country_config(iso3=ISO3)
