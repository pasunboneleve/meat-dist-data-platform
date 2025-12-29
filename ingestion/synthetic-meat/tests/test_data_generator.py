import polars as pl
from unittest.mock import patch, Mock
from synthetic_meat.main import (
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
    """Tests the synthetic data generation logic."""
    base_df = load_base_data(fixture_path)
    num_records = 50
    synthetic_df = generate_synthetic_carcasses(base_df, num_records=num_records)

    assert isinstance(synthetic_df, pl.DataFrame)
    assert len(synthetic_df) == num_records
    
    expected_cols = [
        "carcass_id", "rfid_tag", "slaughter_date", "plant_id", 
        "animal_class", "hscw_kg", "price_aud_per_kg", "quality_score",
        "marbling_score", "fat_depth_mm", "total_price_aud"
    ]
    assert all(col in synthetic_df.columns for col in expected_cols)
    
    # Check data types
    assert synthetic_df["carcass_id"].dtype == pl.String
    assert synthetic_df["hscw_kg"].dtype == pl.Float64
    assert synthetic_df["total_price_aud"].dtype == pl.Float64
    
    # Check for nulls
    assert synthetic_df.select(pl.all().is_null().sum()).row(0) == tuple([0] * len(synthetic_df.columns))


@patch("synthetic_meat.main.write_to_gcs")
def test_generate_and_upload_success(mock_write_to_gcs, fixture_path):
    """Tests the main cloud function entry point on a successful run."""
    # Arrange
    # fixture_path ensures the fixture file is available for load_base_data
    mock_request = Mock()

    # Act
    response, status_code = generate_and_upload(mock_request)

    # Assert
    assert status_code == 200
    assert response == "Data generation and upload complete."
    mock_write_to_gcs.assert_called_once()

    # Check that write_to_gcs was called with a dataframe with expected number of rows
    call_args = mock_write_to_gcs.call_args[0]
    assert isinstance(call_args[0], pl.DataFrame)
    assert len(call_args[0]) == 1000


@patch("synthetic_meat.main.load_base_data", side_effect=Exception("Test error"))
def test_generate_and_upload_failure(mock_load_base_data):
    """Tests the main cloud function entry point on a failure when loading data."""
    # Arrange
    mock_request = Mock()

    # Act
    response, status_code = generate_and_upload(mock_request)

    # Assert
    assert status_code == 500
    assert "An internal error occurred: Test error" in response
    mock_load_base_data.assert_called_once()
