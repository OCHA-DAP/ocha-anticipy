"""Fixtures applied to all tests."""
import pytest

FAKE_AA_DATA_DIR = "fake_aa_dir"


@pytest.fixture(scope="session", autouse=True)
def mock_aa_data_dir(session_mocker):
    """Mock out the AA_DATA_DIR environment variable."""
    session_mocker.patch.dict(
        "aatoolbox.config.pathconfig.os.environ",
        {"AA_DATA_DIR": FAKE_AA_DATA_DIR},
    )
