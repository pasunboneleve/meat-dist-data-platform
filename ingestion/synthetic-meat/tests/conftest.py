from pathlib import Path
import pytest

@pytest.fixture(scope="session")
def fixture_path() -> Path:
    """Returns the path to the test fixture file."""
    path = Path(__file__).parent / "fixtures" / "market_report.xlsx"
    if not path.exists():
        pytest.fail(
            f"Fixture file not found at {path}. "
            "Please run 'python ingestion/synthetic-meat/download_fixture.py' first."
        )
    return path
