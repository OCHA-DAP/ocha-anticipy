"""Fixtures for all pipeline-related tests."""
import pytest

from aatoolbox.pipeline import Pipeline
from aatoolbox.utils.io import parse_yaml

CONFIG_FILE = "tests/pipeline/fake_config.yaml"
ISO3 = "abc"


@pytest.fixture(scope="session", autouse=True)
def mock_aa_data_dir(tmp_path_factory, session_mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    mock_aa_data_dir_path = tmp_path_factory.mktemp(
        basename="test_aa_data_dir"
    )
    session_mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": str(mock_aa_data_dir_path)},
    )
    return mock_aa_data_dir_path


@pytest.fixture
def pipeline(mocker):
    """Fixture for pipeline with test config params."""
    mocker.patch(
        "aatoolbox.config.countryconfig.parse_yaml",
        return_value=parse_yaml(CONFIG_FILE),
    )
    return Pipeline(iso3_unvalidated=ISO3)
