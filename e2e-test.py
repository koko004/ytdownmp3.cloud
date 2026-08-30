import asyncio
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("1. Loading page...")
        await page.goto("http://localhost:3000", wait_until="networkidle")

        title = await page.title()
        print(f"   Title: {title}")

        meta_desc = await page.eval_on_selector('meta[name="description"]', "el => el.content")
        print(f"   Meta: {meta_desc[:50]}...")

        main_el = await page.query_selector("main")
        print(f"   <main> landmark: {'present' if main_el else 'MISSING'}")

        logo = await page.query_selector("img.logo-img")
        width = await logo.get_attribute("width")
        height = await logo.get_attribute("height")
        fetch_pri = await logo.get_attribute("fetchpriority")
        loading = await logo.get_attribute("loading")
        print(f"   Logo: {width}x{height}, fetchpriority={fetch_pri}, loading={loading}")

        print("\n2. Testing conversion flow...")
        await page.fill("#urlInput", "https://music.youtube.com/watch?v=HgGzn4sgEqc")
        await page.click("#convertBtn")

        print("   Waiting for progress...")
        await page.wait_for_selector(".progress-container.visible", timeout=10000)
        print("   Progress container visible")

        status = "unknown"
        for i in range(40):
            await page.wait_for_timeout(2000)
            progress_text = await page.eval_on_selector("#progressText", "el => el.textContent")
            print(f"   [{i*2}s] {progress_text}")

            download_visible = await page.eval_on_selector(
                "#downloadBtn", "el => el.classList.contains('visible')"
            )
            if download_visible:
                download_url = await page.eval_on_selector(
                    "#downloadBtn", "el => el.dataset.url"
                )
                print(f"   Download button visible! URL: {download_url}")

                # Verify the download URL is reachable
                api_response = await page.evaluate("""async (url) => {
                    const res = await fetch(url, { method: 'HEAD' });
                    return { status: res.status, type: res.headers.get('content-type') };
                }""", download_url)
                print(f"   HEAD {download_url} -> {api_response}")

                if api_response["status"] == 200 and "audio" in (api_response["type"] or ""):
                    status = "completed"
                else:
                    status = "failed"
                break

            error_visible = await page.eval_on_selector(
                "#errorMsg", "el => el.classList.contains('visible')"
            )
            if error_visible:
                error_text = await page.eval_on_selector("#errorMsg", "el => el.textContent")
                print(f"   ERROR: {error_text}")
                status = "failed"
                break

        print(f"\n3. Result: {status}")
        await browser.close()
        return 0 if status == "completed" else 1

exit_code = asyncio.run(main())
import sys; sys.exit(exit_code)
