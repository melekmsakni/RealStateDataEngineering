import asyncio
from playwright.async_api import async_playwright

SBR_WS_CDP = "wss://brd-customer-hl_328c4fd6-zone-scraping_project_1:oxzc9xwnbo0y@brd.superproxy.io:9222"
BASE_URL = "https://www.tecnocasa.tn/"


async def run(pw):
    print("Connecting to Scraping Browser...")
    # browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)

    browser = await pw.chromium.launch()
    context = await browser.new_context()

    try:
        # page = await browser.new_page()
        # vdvdfvfdvdvdv
        page = await context.new_page()
        print(f"Connected! Navigating to {BASE_URL}")
        await page.goto(BASE_URL)

        await page.locator("#input-geo-select-province div").filter(
            has_text="Select option"
        ).nth(1).click()
        await page.locator("span").filter(has_text="Grand Tunis").first.click()
        await page.locator("form").get_by_role("button").click()

        print("Navigated to Tunis Scraping page content...")
        html = await page.content()
        print(html)
    finally:
        await browser.close()


async def main():
    async with async_playwright() as playwright:
        await run(playwright)


if __name__ == "__main__":
    asyncio.run(main())
