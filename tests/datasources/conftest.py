"""Fixtures for all pipeline-related tests."""
import pytest

from ochanticipy import GeoBoundingBox, create_custom_country_config
from ochanticipy.config.pathconfig import BASE_DIR_ENV

CONFIG_FILE = "tests/datasources/fake_config.yaml"
ISO3 = "abc"
ISO2 = "ab"


@pytest.fixture(autouse=True)
def mock_aa_data_dir(tmp_path_factory, mocker):
    """Mock out the base directory environment variable."""
    mock_aa_data_dir_path = tmp_path_factory.mktemp(
        basename="test_aa_data_dir"
    )
    mocker.patch.dict(
        "ochanticipy.config.pathconfig.os.environ",
        {BASE_DIR_ENV: str(mock_aa_data_dir_path)},
    )
    return mock_aa_data_dir_path


@pytest.fixture
def mock_country_config():
    """Fixture for pipeline with test config params."""
    return create_custom_country_config(filepath=CONFIG_FILE)


@pytest.fixture
def geo_bounding_box():
    """Input GeoBoundingBox to use."""
    gbb = GeoBoundingBox(lat_max=1.0, lat_min=-2.2, lon_max=3.3, lon_min=-4.4)
    return gbb


def pytest_configure(config):
    """Create custom markers to add to tests."""
    config.addinivalue_line(
        "markers",
        (
            "nomockiso2: do not mock the get_iso2_from_iso3 function. "
            "Used for FEWS NET"
        ),
    )
