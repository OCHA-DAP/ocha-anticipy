"""Fixtures applied to all tests."""
import pytest

from aatoolbox.pipeline import Pipeline
from aatoolbox.utils.io import parse_yaml

ISO3 = "abc"
CONFIG_FILE = "tests/fake_config.yaml"
FAKE_AA_DATA_DIR = "fake_aa_dir"


@pytest.fixture(scope="session", autouse=True)
def mock_aa_data_dir(session_mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    session_mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": FAKE_AA_DATA_DIR},
    )


@pytest.fixture
def pipeline_caller(mocker):
    """Fixture for pipeline with test config params."""

    def _pipeline_caller(config_dict: dict = None):
        if config_dict is None:
            config_dict = parse_yaml(CONFIG_FILE)
        mocker.patch(
            "aatoolbox.config.countryconfig.parse_yaml",
            return_value=config_dict,
        )
        return Pipeline(iso3_unvalidated=ISO3)

    return _pipeline_caller
