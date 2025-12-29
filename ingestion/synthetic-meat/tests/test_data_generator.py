import polars as pl
from src.synthetic_meat.main import load_base_data, generate_synthetic_carcasses

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
