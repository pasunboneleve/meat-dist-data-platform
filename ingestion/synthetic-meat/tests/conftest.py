from pathlib import Path
import pytest
import requests

MLA_API_URL = "https://api-mlastatistics.mla.com.au"


def _download_fixture(path: Path):
    """
    Downloads the latest XLSX cattle market report from the MLA website's API
    to serve as a test fixture.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "request": {
            "search": "",
            "searchType": "site-search",
            "categories": ["Market Reports"],
            "tags": ["Cattle"],
            "fileTypes": ["XLSX"],
            "page": 1,
            "pageSize": 1,
            "sortBy": "date",
            "sortOrder": "desc",
        }
    }

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Referer": "https://www.mla.com.au/",
    }

    print(f"\nRequesting data from {MLA_API_URL}...")
    try:
        response = requests.post(MLA_API_URL, json=payload, headers=headers)
        response.raise_for_status()

        data = response.json()
        results = data.get("data", {}).get("results", [])
        if not results:
            raise RuntimeError("No market reports found in the API response.")

        download_url = results[0].get("fileUrl")
        if not download_url:
            raise RuntimeError("Could not find a download URL in the first report.")

        print(f"Downloading fixture from: {download_url}")
        file_response = requests.get(download_url)
        file_response.raise_for_status()

        with open(path, "wb") as f:
            f.write(file_response.content)

        print(f"Successfully downloaded fixture to {path}")

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred during the request: {e}") from e


@pytest.fixture(scope="session")
def fixture_path() -> Path:
    """
    Returns the path to the test fixture file.
    Downloads the file if it's missing.
    """
    path = Path(__file__).parent / "fixtures" / "market_report.xlsx"
    if not path.exists():
        print(f"Fixture file not found at {path}. Downloading...")
        try:
            _download_fixture(path)
        except Exception as e:
            pytest.fail(f"Failed to download fixture: {e}")

    if not path.exists():
        pytest.fail(f"Fixture file still not found at {path} after attempting download.")

    return path
