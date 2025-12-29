import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, expect

FIXTURE_DIR = Path(__file__).parent / "tests" / "fixtures"
FIXTURE_PATH = FIXTURE_DIR / "market_report.xlsx"
MLA_URL = "https://www.mla.com.au/prices-markets/market-reports-prices/"

async def main():
    """
    Uses Playwright to download an XLSX market report from the MLA website
    to serve as a test fixture.
    """
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        print(f"Navigating to {MLA_URL}...")
        await page.goto(MLA_URL, wait_until="networkidle")

        # The site might have a cookie banner
        try:
            cookie_button = page.get_by_role("button", name="Allow all cookies")
            await cookie_button.wait_for(timeout=5000)
            if await cookie_button.is_visible():
                await cookie_button.click()
                print("Clicked cookie consent button.")
        except Exception:
            print("Cookie banner not found or could not be clicked.")

        print("Applying filters...")
        await page.get_by_role("checkbox", name="Cattle", exact=True).check()
        await page.get_by_role("checkbox", name="XLSX", exact=True).check()

        # Wait for the results to load by checking for report items.
        await expect(page.locator("div.search-results-list-item-component")).to_have_count(
            lambda c: c > 0, timeout=20000
        )
        print("Filter results loaded.")
        
        first_report = page.locator("div.search-results-list-item-component").first
        
        async with page.expect_download() as download_info:
            print("Clicking download link on the first report...")
            await first_report.get_by_role("link", name="Download").click()
        
        download = await download_info.value
        await download.save_as(FIXTURE_PATH)
        print(f"Successfully downloaded fixture to {FIXTURE_PATH}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
