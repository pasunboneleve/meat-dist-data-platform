import polars as pl
from unittest.mock import patch, Mock
from datetime import date
from synthetic_meat.core import (
    load_base_data,
    generate_synthetic_carcasses,
    generate_and_upload,
)


def test_load_base_data(fixture_path):
    """Tests that the base data can be loaded from the fixture."""
    df = load_base_data(fixture_path)
    assert isinstance(df, pl.DataFrame)
    assert not df.is_empty()
    assert "category" in df.columns

def test_generate_synthetic_carcasses(fixture_path):
    """Tests the synthetic data generation logic using the latest date in the fixture."""
    base_df = load_base_data(fixture_path)

    # Use the most recent date from the fixture for the test
    latest_date_str = base_df.select(pl.max("report_date")).item()
    target_date = date.fromisoformat(latest_date_str.split("T")[0])

    synthetic_df = generate_synthetic_carcasses(base_df, target_date=target_date)

    assert isinstance(synthetic_df, pl.DataFrame)

    expected_cols = [
        "carcass_id",
        "rfid_tag",
        "slaughter_date",
        "plant_id",
        "animal_class",
        "hscw_kg",
        "price_aud_per_kg",
        "quality_score",
        "marbling_score",
        "fat_depth_mm",
        "total_price_aud",
    ]
    assert all(col in synthetic_df.columns for col in expected_cols)

    # If records were generated, check types, nulls, and dates
    if not synthetic_df.is_empty():
        # Check data types
        assert synthetic_df["carcass_id"].dtype == pl.String
        assert synthetic_df["hscw_kg"].dtype == pl.Float64
        assert synthetic_df["total_price_aud"].dtype == pl.Float64

        # Check for nulls
        assert synthetic_df.select(pl.all().is_null().sum()).row(0) == tuple(
            [0] * len(synthetic_df.columns)
        )

        # Check that slaughter dates match the target date
        slaughter_dates = synthetic_df["slaughter_date"].str.to_datetime().dt.date().unique()
        assert len(slaughter_dates) == 1
        assert slaughter_dates[0] == target_date


def test_generated_data_matches_stats(fixture_path):
    """
    Tests that the generated data's statistics (record count and average price)
    are plausible based on the input market data for a specific date.
    """
    base_df = load_base_data(fixture_path)

    # Pick a date from the fixture to test against
    target_stat = base_df.row(len(base_df) // 2, named=True)
    target_date = date.fromisoformat(target_stat["report_date"].split("T")[0])
    expected_records = int(target_stat["head_count"])
    # price is in cents/kg in fixture, we generate aud/kg
    expected_avg_price = float(target_stat["indicator_value"]) / 100.0

    synthetic_df = generate_synthetic_carcasses(base_df, target_date=target_date)

    assert len(synthetic_df) == expected_records

    # The generated price is based on a normal distribution around the target average.
    # It won't be exact, but it should be reasonably close.
    # We'll check if it's within a certain tolerance (e.g., +/- 15%).
    if not synthetic_df.is_empty():
        actual_avg_price = synthetic_df["price_aud_per_kg"].mean()
        assert abs(actual_avg_price - expected_avg_price) / expected_avg_price < 0.15


@patch("synthetic_meat.core.fetch_base_data")
@patch("synthetic_meat.core.write_to_gcs")
def test_generate_and_upload_with_target_date(
    mock_write_to_gcs, mock_fetch_base_data, fixture_path
):
    """Tests the main cloud function with a specific target_date parameter."""
    # Arrange
    base_df = load_base_data(fixture_path)
    mock_fetch_base_data.return_value = base_df

    # Pick a date from the middle of the fixture data for a reliable test
    target_stat = base_df.row(len(base_df) // 2, named=True)
    target_date_str = target_stat["report_date"].split("T")[0]
    expected_records = int(target_stat["head_count"])

    mock_request = Mock()
    mock_request.args = {"target_date": target_date_str}
    mock_request.is_json = False

    # Act
    response, status_code = generate_and_upload(mock_request)

    # Assert
    assert status_code == 200
    assert response == "Data generation and upload complete."
    mock_write_to_gcs.assert_called_once()

    call_args = mock_write_to_gcs.call_args[0]
    generated_df, _, _, call_date = call_args

    assert isinstance(generated_df, pl.DataFrame)
    assert len(generated_df) == expected_records
    assert call_date == date.fromisoformat(target_date_str)


@patch("synthetic_meat.core.fetch_base_data", side_effect=Exception("Test error"))
def test_generate_and_upload_failure(mock_fetch_base_data):
    """Tests the main cloud function entry point on a failure when loading data."""
    # Arrange
    mock_request = Mock()
    mock_request.args = {}  # To avoid TypeError on 'in' operator
    mock_request.is_json = False

    # Act
    response, status_code = generate_and_upload(mock_request)

    # Assert
    assert status_code == 500
    assert "An internal error occurred: Test error" in response
    mock_fetch_base_data.assert_called_once()
