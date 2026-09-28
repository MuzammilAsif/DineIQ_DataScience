"""Captures one screenshot per page of the running app with headless Chromium.

Start the app first: .venv/bin/streamlit run src/app.py --server.port 8599
Then run:           .venv/bin/python screenshots/take_screenshots.py [base_url]
Pages are opened through the sidebar links, so the logged-in session is kept.
"""
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent
BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8599"
PAGES = ["Executive Dashboard", "Menu Intelligence", "Customer Intelligence", "Demand Forecast",
         "Wastage", "Anomalies", "Recommendations", "What-If Simulator", "Model Comparison",
         "Admin"]


def settle(page):
    """Waits until the script has finished: no loading skeletons and a stable page height."""
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(1500)
    last, stable = -1, 0
    for _ in range(240):
        busy = page.evaluate(
            "() => !!document.querySelector('[data-testid=\"stSkeleton\"], "
            "[data-testid=\"stSpinner\"]') || document.body.innerText.includes('Running...')")
        h = page.evaluate("() => (document.querySelector('[data-testid=\"stMain\"]') "
                          "|| document.body).scrollHeight")
        stable = stable + 1 if (h == last and not busy) else 0
        if stable >= 4:
            break
        last = h
        page.wait_for_timeout(500)
    page.wait_for_timeout(1000)


def _height(page):
    return page.evaluate(
        "() => Math.max(900, ...['[data-testid=\"stMain\"]', '[data-testid=\"stMainBlockContainer\"]']"
        ".map(s => document.querySelector(s)).filter(Boolean).map(e => e.scrollHeight))")


def shoot(page, name):
    for _ in range(2):
        page.set_viewport_size({"width": 1440, "height": min(_height(page) + 80, 12000)})
        settle(page)
    page.screenshot(path=str(OUT / f"{name}.png"))
    page.set_viewport_size({"width": 1440, "height": 900})
    print("saved", name)


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 900})
        page.goto(BASE)
        settle(page)
        shoot(page, "00_login")
        page.get_by_role("textbox", name="Username").fill("admin")
        page.get_by_role("textbox", name="Password").fill("Admin@123")
        page.get_by_role("button", name="Sign in").click()
        settle(page)
        for i, name in enumerate(PAGES, 1):
            page.locator('[data-testid="stSidebarNav"]').get_by_text(name, exact=True).click()
            settle(page)
            shoot(page, f"{i:02d}_{name.lower().replace(' ', '_').replace('-', '')}")
        browser.close()


if __name__ == "__main__":
    main()
