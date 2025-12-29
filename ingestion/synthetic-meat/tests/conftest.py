from pathlib import Path
import pytest
import requests
import json
from datetime import datetime, timedelta

MLA_API_URL = "https://api-mlastatistics.mla.com.au"


def _create_fixture(path: Path):
    """
    Creates a test fixture by downloading cattle pricing data from the MLA Statistics API.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    to_date = datetime.utcnow()
    # Fetch data from the last 90 days for a decent sample size.
    from_date = to_date - timedelta(days=90)

    from_date_str = from_date.strftime("%Y-%m-%d")
    to_date_str = to_date.strftime("%Y-%m-%d")

    # Fetch National Feeder Steer Indicator (3) from Dalby saleyard (DAL).
    # This provides a consistent set of real-world price and volume data.
    endpoint = "/report/6"
    params = {
        "indicatorID": 3,
        "saleyardID": "DAL",
        "fromDate": from_date_str,
        "toDate": to_date_str,
    }

    print(f"\nRequesting data from {MLA_API_URL}{endpoint} with params: {params}")
    try:
        response = requests.get(f"{MLA_API_URL}{endpoint}", params=params)
        response.raise_for_status()

        data = response.json()
        results = data.get("data", [])

        if not results:
            raise RuntimeError("No data found in API response for the given parameters.")

        with open(path, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Successfully created fixture with {len(results)} records at {path}")

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred during the request: {e}") from e


@pytest.fixture(scope="session")
def fixture_path() -> Path:
    """
    Returns the path to the test fixture file.
    Creates the fixture by querying the API if it's missing.
    """
    path = Path(__file__).parent / "fixtures" / "market_data.json"
    if not path.exists():
        print(f"Fixture file not found at {path}. Creating from API...")
        try:
            _create_fixture(path)
        except Exception as e:
            pytest.fail(f"Failed to create fixture: {e}")

    if not path.exists():
        pytest.fail(f"Fixture file still not found at {path} after attempting to create it.")

    return path
