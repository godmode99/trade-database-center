# python/fetch/calendar/02_capture_document_html.py
#
# Purpose:
# - Open https://www.forexfactory.com/calendar using an existing Playwright storage_state (ff_storage.json)
# - Capture the *document* response HTML (network snapshot) and save it to python/Data/raw_data/calendar/calendar_document.html
# - Save a debug screenshot + small metadata JSON for reproducibility
#
# Notes:
# - Keep console output ASCII-only (Windows cp1252 safe).
# - This script does NOT parse events. It only captures the raw HTML snapshot.

from __future__ import annotations

import json
import os
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from playwright.sync_api import sync_playwright

from utils import load_config


# -----------------------
# Config
# -----------------------
DEFAULT_URL = "https://www.forexfactory.com/calendar"

# Project-relative paths (run from repo root recommended)
STATE_PATH = Path("ff_storage.json")
CONFIG_PATH = Path(__file__).resolve().parent / "app" / "config.yaml"

ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"
OUT_HTML = ART_DIR / "calendar_document.html"
OUT_PNG = ART_DIR / "document_debug.png"
OUT_META = ART_DIR / "calendar_document.meta.json"
OUT_ERR = ART_DIR / "capture_error.txt"


@dataclass
class Meta:
    fetched_at_utc: str
    cwd: str
    url: str
    final_url: str
    page_title: str
    html_saved_to: str
    screenshot_saved_to: str
    storage_state_path: str
    playwright_user_agent: str
    note: str


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)


def _abs(p: Path) -> str:
    return str(p.resolve())


def _load_url() -> str:
    if CONFIG_PATH.exists():
        cfg = load_config(str(CONFIG_PATH))
        url = (cfg.get("capture_document_html", {}) or {}).get("url", "")
        if isinstance(url, str) and url.strip():
            return url.strip()
    return DEFAULT_URL


def main() -> None:
    url = _load_url()
    _ensure_dirs()

    if not STATE_PATH.exists():
        raise FileNotFoundError(
            f"Missing storage state file: {_abs(STATE_PATH)}\n"
            "Expected: ff_storage.json in repo root (or change STATE_PATH in script)."
        )

    html_text: Optional[str] = None
    doc_status: Optional[int] = None
    doc_headers: dict[str, str] = {}
    final_url = ""
    page_title = ""
    ua = ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # set True later if you want

        # Create context from storage_state (session/cookies)
        context = browser.new_context(
            storage_state=str(STATE_PATH),
            viewport={"width": 1400, "height": 900},
            locale="en-US",
        )

        # Capture the user agent for meta
        try:
            ua = context.user_agent
        except Exception:
            ua = ""

        page = context.new_page()

        # Capture the main document HTML from network response
        def on_response(resp):
            nonlocal html_text, doc_status, doc_headers
            try:
                # We only want the top-level document for /calendar
                if resp.request.resource_type == "document" and resp.url.startswith(url):
                    doc_status = resp.status
                    # headers can help debugging
                    doc_headers = {k.lower(): v for k, v in (resp.headers or {}).items()}
                    if resp.status == 200:
                        html_text = resp.text()
            except Exception:
                # swallow; we'll validate later
                pass

        page.on("response", on_response)

        print("goto:", url, flush=True)
        page.goto(url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(4000)

        final_url = page.url
        page_title = page.title()

        # Save a debug screenshot (useful to confirm not "Just a moment...")
        page.screenshot(path=str(OUT_PNG), full_page=True)

        # Close browser resources
        context.close()
        browser.close()

    # Validate capture
    if not html_text:
        # Write a helpful error file
        msg = (
            "Failed to capture document HTML.\n"
            f"- doc_status: {doc_status}\n"
            f"- final_url: {final_url}\n"
            f"- title: {page_title}\n"
            f"- state: {_abs(STATE_PATH)}\n"
            "Try re-generating ff_storage.json and rerun.\n"
        )
        # Include relevant headers if any
        if doc_headers:
            msg += "\nDocument response headers (lowercased):\n"
            msg += json.dumps(doc_headers, indent=2, ensure_ascii=False)
            msg += "\n"

        OUT_ERR.write_text(msg, encoding="utf-8")
        raise RuntimeError("No HTML captured. See python/Data/raw_data/calendar/capture_error.txt")

    # Save HTML snapshot
    OUT_HTML.write_text(html_text, encoding="utf-8")

    # Save meta
    meta = Meta(
        fetched_at_utc=_iso_utc_now(),
        cwd=os.getcwd(),
        url=url,
        final_url=final_url,
        page_title=page_title,
        html_saved_to=_abs(OUT_HTML),
        screenshot_saved_to=_abs(OUT_PNG),
        storage_state_path=_abs(STATE_PATH),
        playwright_user_agent=ua,
        note="Captured top-level document HTML for ForexFactory calendar page (network snapshot).",
    )
    OUT_META.write_text(json.dumps(asdict(meta), indent=2, ensure_ascii=False), encoding="utf-8")

    # Minimal ASCII-only console output
    print("OK saved html:", _abs(OUT_HTML), flush=True)
    print("OK saved png :", _abs(OUT_PNG), flush=True)
    print("OK saved meta:", _abs(OUT_META), flush=True)
    print("title:", page_title, flush=True)
    print("final url:", final_url, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        _ensure_dirs()
        # Write full traceback for debugging
        OUT_ERR.write_text(traceback.format_exc(), encoding="utf-8")
        print("ERROR saved ->", _abs(OUT_ERR), flush=True)
        # Avoid non-ascii prompt that can crash on cp1252
        input("Press Enter to exit...")
