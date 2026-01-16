from datetime import date

import polars as pl

from src.synthesise import generate_synthetic_carcasses


def test_generate_synthetic_carcasses_columns_regression():
    """
    Regression test to ensure all expected columns are present in the output of
    generate_synthetic_carcasses. If you add/remove a column, update this test.
    """
    # Arrange: Create a minimal base DataFrame required by the function
    base_df = pl.DataFrame(
        {
            "head_count": [5],
            "indicator_value": [700.0],
            "category": ["yearling steer"],
            "indicator_id": [1],
            "saleyard_id": [42],
        }
    )
    target_date = date(2025, 1, 17)

    # Act: Generate the synthetic data
    result_df = generate_synthetic_carcasses(base_df, target_date)

    # Assert: Check that the output DataFrame has the expected columns
    expected_columns = {
        "carcass_id",
        "rfid_tag",
        "slaughter_date",
        "plant_id",
        "animal_class",
        "indicator_id",
        "saleyard_id",
        "hscw_kg",
        "price_aud_per_kg",
        "total_price_aud",
        "quality_score",
        "marbling_score",
        "fat_depth_mm",
    }
    actual_columns = set(result_df.columns)

    assert actual_columns == expected_columns
