#!/bin/env python

from pathlib import Path

import requests

FIXTURE_DIR = Path(__file__).parent / "tests" / "fixtures"
FIXTURE_PATH = FIXTURE_DIR / "market_report.xlsx"
MLA_API_URL = "https://www.mla.com.au/api/search/site-search"


def main():
    """
    Downloads the latest XLSX cattle market report from the MLA website's API
    to serve as a test fixture. This avoids the need for a browser.
    """
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

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
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    }

    print(f"Requesting data from {MLA_API_URL}...")
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

        print(f"Downloading file from: {download_url}")
        file_response = requests.get(download_url)
        file_response.raise_for_status()

        with open(FIXTURE_PATH, "wb") as f:
            f.write(file_response.content)

        print(f"Successfully downloaded fixture to {FIXTURE_PATH}")

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"An error occurred during the request: {e}") from e


if __name__ == "__main__":
    main()
