"""Test the GloFAS instantiation."""
import sys
from datetime import date

import pytest

from ochanticipy import GlofasForecast, GlofasReanalysis, GlofasReforecast


def test_reanalysis_dates(mock_country_config, geo_bounding_box):
    """Test date range behaviour of reanalysis."""

    def glofas_reanalysis(end_date: date, start_date: date = None):
        return GlofasReanalysis(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
        )

    # Try using a string as a date
    good_end_date = "2022-11-15"
    # These should not throw an error
    glofas_reanalysis(end_date=good_end_date)
    # End date too far in future
    glofas_future = glofas_reanalysis(end_date=date(year=3000, month=1, day=1))
    assert glofas_future._end_date == date.today()
    # Start date too early
    glofas_past = glofas_reanalysis(
        start_date=date(year=1800, month=1, day=1), end_date=good_end_date
    )
    assert glofas_past._start_date == date(year=1979, month=1, day=1)
    # End date > start date
    with pytest.raises(ValueError):
        glofas_reanalysis(
            start_date=date(year=2020, month=1, day=2),
            end_date=date(year=2020, month=1, day=1),
        )


def test_forecast_dates(mock_country_config, geo_bounding_box):
    """Test date range behaviour of forecast."""

    def glofas_forecast(end_date: date, start_date: date = None):
        return GlofasForecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
            leadtime_max=15,
        )

    # Try using a string as a date
    good_end_date = "2022-05-15"
    # These should not throw an error
    glofas_forecast(end_date=good_end_date)
    # End date too far in future
    glofas_future = glofas_forecast(
        start_date=date.today(), end_date=date(year=3000, month=1, day=1)
    )
    assert glofas_future._end_date == date.today()
    # Start date too early
    glofas_past = glofas_forecast(
        start_date=date(year=1800, month=1, day=1), end_date=good_end_date
    )
    assert glofas_past._start_date == date(year=2021, month=5, day=26)
    # End date > start date
    with pytest.raises(ValueError):
        glofas_forecast(
            start_date=date(year=2020, month=1, day=2),
            end_date=date(year=2020, month=1, day=1),
        )


def test_reforecast_dates(mock_country_config, geo_bounding_box):
    """Test date range behaviour of reforecast."""

    def glofas_reforecast(start_date: date = None, end_date: date = None):
        return GlofasReforecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
            leadtime_max=15,
        )

    def glofas_reforecast_v3(start_date: date = None, end_date: date = None):
        return GlofasReforecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            start_date=start_date,
            end_date=end_date,
            leadtime_max=15,
            model_version=3,
        )

    # These should not throw an error
    glofas_reforecast()
    # Try using a string as a date
    glofas_reforecast(end_date="2010-01-01")
    # End date too far in future
    glofas_future = glofas_reforecast(end_date=date(year=3000, month=1, day=1))
    assert glofas_future._end_date == date(year=2022, month=8, day=31)
    glofas_future_v3 = glofas_reforecast_v3(
        end_date=date(year=3000, month=1, day=1)
    )
    assert glofas_future_v3._end_date == date(year=2018, month=12, day=31)
    # Start date too early
    glofas_past = glofas_reforecast(start_date=date(year=1800, month=1, day=1))
    assert glofas_past._start_date == date(year=2003, month=3, day=1)
    glofas_past_v3 = glofas_reforecast_v3(
        start_date=date(year=1800, month=1, day=1)
    )
    assert glofas_past_v3._start_date == date(year=1999, month=1, day=1)
    # End date > start date
    with pytest.raises(ValueError):
        glofas_reforecast(
            start_date=date(year=2020, month=1, day=2),
            end_date=date(year=2020, month=1, day=1),
        )
    # Date range only has forbidden months
    with pytest.raises(ValueError):
        glofas_reforecast(
            start_date=date(year=2020, month=9, day=1),
            end_date=date(year=2021, month=2, day=1),
        )


def test_optional_module_error(
    mock_country_config, geo_bounding_box, monkeypatch
):
    """Test module error raised correctly if dependencies missing."""
    monkeypatch.setitem(sys.modules, "cdsapi", None)  # noqa: FKA01
    with pytest.raises(ModuleNotFoundError, match=r"ochanticipy"):
        from ochanticipy import GlofasForecast

        GlofasForecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            leadtime_max=1,
        )


def test_optional_module_no_error(monkeypatch):
    """Test no errors generated on library import w/o dependencies."""
    monkeypatch.setitem(sys.modules, "cdsapi", None)  # noqa: FKA01
    import ochanticipy  # noqa: F401


def test_max_requests(mock_country_config, geo_bounding_box):
    """Test that max request number can't be exceeded."""
    with pytest.raises(RuntimeError):
        GlofasForecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            leadtime_max=15,
            start_date=date(year=2021, month=5, day=26),
            end_date=date(year=2023, month=5, day=29),
        )


def test_incorrect_model_version(mock_country_config, geo_bounding_box):
    """Test that incorrect model version raises an error."""
    with pytest.raises(ValueError):
        GlofasForecast(
            country_config=mock_country_config,
            geo_bounding_box=geo_bounding_box,
            leadtime_max=15,
            model_version=10,
        )
