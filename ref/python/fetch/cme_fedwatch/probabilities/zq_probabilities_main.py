# python/fetch/cme_fedwatch/probabilities_main.py
import argparse
import asyncio
import html
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta, tzinfo
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

from playwright.async_api import async_playwright, Browser, Page, Response, Error as PWError

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python < 3.9
    ZoneInfo = None

BASE_DIR = Path(__file__).resolve().parent
PYTHON_DIR = BASE_DIR.parents[2].resolve()
REPO_ROOT = PYTHON_DIR.parent
TELEGRAM_REPORT_DIR = PYTHON_DIR / "telegram_report"

if TELEGRAM_REPORT_DIR.exists() and str(TELEGRAM_REPORT_DIR) not in os.sys.path:
    os.sys.path.insert(0, str(TELEGRAM_REPORT_DIR))

from telegram_notifier import send_telegram_message

DEFAULT_TZ = "Asia/Bangkok"
SOURCE_TZ_MAP = {
    "CT": "America/Chicago",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
}


def time_stamp(tzinfo: tzinfo) -> str:
    return datetime.now(tzinfo).strftime("%Y%m%d_%H%M%S")


def now_local_iso(tzinfo: tzinfo) -> str:
    return datetime.now(tzinfo).replace(microsecond=0).isoformat()


def load_env_file(start_dir: Path) -> None:
    for parent in (start_dir, *start_dir.parents):
        env_path = parent / ".env"
        if env_path.exists():
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    os.environ[key] = value
            break


def inject_telegram_env(cfg: dict) -> dict:
    cfg = dict(cfg or {})
    telegram = cfg.get("telegram", {}) or {}

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if tg_token:
        telegram["bot_token"] = tg_token

    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if tg_chat:
        telegram["chat_id"] = tg_chat

    cfg["telegram"] = telegram
    return cfg


def load_config(config_path: Path) -> dict:
    load_env_file(REPO_ROOT)
    cfg = {"timezone": DEFAULT_TZ, "telegram": {"enabled": True}}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        cfg.update(loaded or {})
        if "telegram" in loaded:
            cfg["telegram"] = loaded.get("telegram") or cfg.get("telegram", {})
    return inject_telegram_env(cfg)


def resolve_timezone(cfg: dict, logger: logging.Logger | None = None) -> tuple[tzinfo, str]:
    tz_name = (cfg.get("timezone") or DEFAULT_TZ).strip()
    if ZoneInfo:
        try:
            return ZoneInfo(tz_name), tz_name
        except Exception as exc:
            if logger:
                logger.warning("Invalid timezone %s; fallback to Asia/Bangkok: %s", tz_name, exc)
    return timezone(timedelta(hours=7)), DEFAULT_TZ


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("cme_fedwatch_probabilities")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def safe_name(s: str, max_len: int = 120) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "_", s)
    return s[:max_len].strip("_") or "resp"


def build_capture_name(url: str, status: int, rtype: str, ctype: str, tzinfo: tzinfo) -> str:
    parsed = urlparse(url)
    endpoint = Path(parsed.path).name or parsed.netloc or "response"
    suffix = ".json" if "json" in ctype.lower() else ".txt"
    timestamp = time_stamp(tzinfo)
    base = safe_name(f"fedwatch_{rtype}_{endpoint}_{status}_{timestamp}")
    return f"{base}{suffix}"


DOC3_MARKER = '<div id="doc3" class="do-mobile min-width-template">'


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def strip_tags(text: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", text)
    cleaned = html.unescape(cleaned)
    cleaned = cleaned.replace("\xa0", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_pct(value: str) -> Optional[float]:
    cleaned = value.strip().replace("%", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: str) -> Optional[int]:
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def extract_rows(table_html: str) -> Sequence[str]:
    return re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.S)


def extract_cells(row_html: str) -> Sequence[str]:
    return re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.S)


def parse_column_header(text: str) -> dict:
    cleaned = strip_tags(text).replace("*", "").strip()
    parts = cleaned.split()
    if not parts:
        return {"key": "unknown", "label": cleaned}
    if parts[0].isdigit() and len(parts) >= 2:
        label = " ".join(parts[:2])
        date = " ".join(parts[2:]).strip()
    else:
        label = parts[0]
        date = " ".join(parts[1:]).strip()
    key_map = {
        "Now": "now",
        "1 Day": "1_day",
        "1 Week": "1_week",
        "1 Month": "1_month",
    }
    entry = {"key": key_map.get(label, label.lower().replace(" ", "_")), "label": label}
    if date:
        entry["date"] = date
    return entry


def convert_as_of_to_thai(as_of_text: str, timezone_text: str) -> Optional[tuple[str, str]]:
    if not as_of_text or not timezone_text:
        return None
    source_zone = SOURCE_TZ_MAP.get(timezone_text.strip().upper())
    if not source_zone or not ZoneInfo:
        return None
    match = re.match(r"^(.*)\s([A-Za-z]{2,4})$", as_of_text.strip())
    if not match:
        return None
    date_part = match.group(1).strip()
    try:
        parsed = datetime.strptime(date_part, "%d %b %Y %H:%M:%S")
    except ValueError:
        return None
    localized = parsed.replace(tzinfo=ZoneInfo(source_zone))
    target_tz = ZoneInfo(DEFAULT_TZ)
    converted = localized.astimezone(target_tz)
    return converted.strftime("%d %b %Y %H:%M:%S"), DEFAULT_TZ


def parse_quikstrike_html(body: str) -> Optional[dict]:
    if DOC3_MARKER not in body:
        return None

    meeting_match = re.search(r"<th colspan=\"6\">Meeting Information</th>(.*?)</table>", body, re.S)
    probs_match = re.search(r"<th colspan=\"3\">Probabilities</th>(.*?)</table>", body, re.S)
    target_table_match = re.search(r"<table class=\"grid-thm grid-thm-v2 w-lg\">(.*?)</table>", body, re.S)

    if not meeting_match or not probs_match or not target_table_match:
        return None

    meeting_table = meeting_match.group(1)
    probs_table = probs_match.group(1)
    target_table = target_table_match.group(1)

    meeting_row = None
    for row in extract_rows(meeting_table):
        cells = [strip_tags(cell) for cell in extract_cells(row)]
        if len(cells) == 6 and cells[0] != "Meeting Date":
            meeting_row = cells
            break

    prob_row = None
    for row in extract_rows(probs_table):
        cells = [strip_tags(cell) for cell in extract_cells(row)]
        if len(cells) == 3 and cells[0] != "Ease":
            prob_row = cells
            break

    header_match = re.search(r"<tr class=\"compact\">(.*?)</tr>", target_table, re.S)
    if not header_match:
        return None
    header_cells = extract_cells(header_match.group(1))
    columns = [parse_column_header(cell) for cell in header_cells]

    rows = []
    current_target_rate = None
    for row in extract_rows(target_table):
        class_match = re.search(r"class=\"([^\"]*)\"", row)
        class_value = class_match.group(1) if class_match else ""
        cells = [strip_tags(cell) for cell in extract_cells(row)]
        if len(cells) != 5:
            continue
        target_rate = cells[0]
        is_current = "(Current)" in target_rate
        if is_current:
            current_target_rate = target_rate.replace(" (Current)", "").strip()
        rows.append(
            {
                "target_rate_bps": target_rate,
                "now_pct": parse_pct(cells[1]),
                "d1_pct": parse_pct(cells[2]),
                "w1_pct": parse_pct(cells[3]),
                "m1_pct": parse_pct(cells[4]),
                "is_current": is_current,
                "is_hidden": "hide" in class_value.split(),
            }
        )

    as_of_match = re.search(r"Data as of\s*([^<]+)", target_table, re.S)
    as_of_text = strip_tags(as_of_match.group(1)) if as_of_match else ""
    timezone_text = as_of_text.split()[-1] if as_of_text else ""
    as_of_thai = convert_as_of_to_thai(as_of_text, timezone_text)

    if not meeting_row or not prob_row:
        return None

    meeting_date, contract_code, contract_expires, mid_price, prior_volume, prior_oi = meeting_row
    ease_pct, no_change_pct, hike_pct = prob_row

    return {
        "source": "cme_quikstrike_view_html",
        "as_of": {
            "text": as_of_thai[0] if as_of_thai else as_of_text,
            "timezone": as_of_thai[1] if as_of_thai else timezone_text,
        },
        "as_of_source": {"text": as_of_text, "timezone": timezone_text},
        "meeting": {
            "date": meeting_date,
            "current_target_rate_bps_range": current_target_rate,
        },
        "contract": {
            "code": contract_code,
            "expires": contract_expires,
            "mid_price": parse_float(mid_price),
            "prior_volume": parse_int(prior_volume),
            "prior_open_interest": parse_int(prior_oi),
        },
        "direction_probabilities_pct": {
            "ease": parse_pct(ease_pct),
            "no_change": parse_pct(no_change_pct),
            "hike": parse_pct(hike_pct),
        },
        "target_rate_probabilities": {
            "columns": columns,
            "rows": rows,
        },
    }


async def dump_response(resp: Response, out_dir: Path, tzinfo: tzinfo) -> Optional[tuple[Path, bool]]:
    """
    Save XHR/fetch responses to disk.
    - If JSON: save pretty JSON
    - Else: save text (best-effort)
    """
    try:
        req = resp.request
        rtype = req.resource_type
        url = resp.url
        status = resp.status
        headers = await resp.all_headers()
        ctype = headers.get("content-type", "") or headers.get("Content-Type", "")

        # we only care about network data, not images/css/fonts
        if rtype not in ("xhr", "fetch", "document"):
            return None

        # Skip obvious static assets even if misclassified
        if any(url.lower().endswith(ext) for ext in (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".woff", ".woff2", ".ttf")):
            return None

        # Create a stable-ish filename
        filename = build_capture_name(url=url, status=status, rtype=rtype, ctype=ctype, tzinfo=tzinfo)
        path = out_dir / filename

        body = await resp.text()
        if DOC3_MARKER not in body:
            return None

        # Try JSON first if content-type suggests it, otherwise try text.
        if "json" in ctype.lower():
            data = json.loads(body)
            path = path.with_suffix(".json")
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            return path, True

        # Some endpoints lie about content-type; try parse json anyway
        body_strip = body.strip()
        if (body_strip.startswith("{") and body_strip.endswith("}")) or (body_strip.startswith("[") and body_strip.endswith("]")):
            try:
                data = json.loads(body_strip)
                path = path.with_suffix(".json")
                path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                return path, True
            except Exception:
                pass

        path.write_text(body, encoding="utf-8", errors="replace")
        parsed = parse_quikstrike_html(body)
        if parsed:
            json_path = path.with_suffix(".json")
            json_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
            return path, True
        return path, False

    except Exception:
        return None


async def attach_sniffer(page: Page, out_dir: Path, tzinfo: tzinfo, stats: dict) -> None:
    async def on_response(resp: Response) -> None:
        saved = await dump_response(resp, out_dir, tzinfo)
        if saved:
            saved_path, has_json = saved
            stats["captures"] += 1
            if has_json:
                stats["json_files"] += 1
            print(f"[CAPTURE] {resp.status} {resp.request.resource_type:8s} -> {saved_path.name}")

    page.on("response", on_response)


async def goto_with_fallback(page: Page, url: str, timeout_ms: int) -> None:
    """
    Try goto. If domcontentloaded fails, try load event. Then try plain goto.
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        return
    except PWError as e:
        print(f"[WARN] goto(domcontentloaded) failed: {e}")

    try:
        await page.goto(url, wait_until="load", timeout=timeout_ms)
        return
    except PWError as e:
        print(f"[WARN] goto(load) failed: {e}")

    # last attempt
    await page.goto(url, timeout=timeout_ms)


async def run_once(
    browser_name: str,
    url: str,
    out_dir: Path,
    wait_s: int,
    save_har: bool,
    timeout_ms: int,
    headed: bool,
    tzinfo: tzinfo,
) -> None:
    ensure_dir(out_dir)
    stats = {"captures": 0, "json_files": 0}

    async with async_playwright() as p:
        bt = {"chromium": p.chromium, "firefox": p.firefox, "webkit": p.webkit}[browser_name]

        launch_args = [
            "--disable-quic",
            # Some environments choke on H2; these flags may or may not help depending on Chromium version.
            "--disable-http2",
            "--disable-features=NetworkService,UseDnsHttpsSvcb,EncryptedClientHello",
        ] if browser_name == "chromium" else []

        print(f"[INFO] Launching {browser_name} (headed={headed})")
        browser: Browser = await bt.launch(
            headless=not headed,
            args=launch_args if launch_args else None,
        )

        har_path = out_dir / f"fedwatch_{browser_name}_{time_stamp(tzinfo)}.har" if save_har else None
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            ignore_https_errors=True,
            record_har_path=str(har_path) if har_path else None,
            record_har_content="embed" if har_path else None,
        )
        page = await context.new_page()

        # Optional: reduce noise; keep XHR/fetch/document
        async def route_filter(route, request):
            rtype = request.resource_type
            if rtype in ("image", "media", "font"):
                return await route.abort()
            return await route.continue_()

        await context.route("**/*", route_filter)

        await attach_sniffer(page, out_dir, tzinfo, stats)

        print(f"[INFO] Opening: {url}")
        await goto_with_fallback(page, url, timeout_ms=timeout_ms)

        # wait for iframe + XHR to fire
        print(f"[INFO] Waiting {wait_s}s to capture network…")
        await page.wait_for_timeout(wait_s * 1000)

        await context.close()
        await browser.close()

        if har_path:
            print(f"[INFO] HAR saved: {har_path}")

    return stats, har_path


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html")
    ap.add_argument("--out", default="python/Data/raw_data/cme/fedwatch_probabilities/zq")
    ap.add_argument("--wait", type=int, default=20)
    ap.add_argument("--save_har", action="store_true")
    ap.add_argument("--timeout_ms", type=int, default=60000)
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--browser", choices=["auto", "chromium", "firefox", "webkit"], default="auto")
    ap.add_argument("--parse_raw", help="Parse a raw QuikStrike HTML file to JSON and exit.")
    ap.add_argument("--config", default=str(BASE_DIR / "probabilities_config.json"))
    args = ap.parse_args()

    if args.parse_raw:
        raw_path = Path(args.parse_raw)
        raw_body = raw_path.read_text(encoding="utf-8", errors="replace")
        parsed = parse_quikstrike_html(raw_body)
        if not parsed:
            raise SystemExit(f"Failed to parse {raw_path}")
        json_path = raw_path.with_suffix(".json")
        json_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[DONE] Parsed JSON saved: {json_path}")
        return

    logger = setup_logger()
    cfg = load_config(Path(args.config))
    tzinfo, tz_label = resolve_timezone(cfg, logger=logger)

    out_dir = Path(args.out)
    ensure_dir(out_dir)

    browsers = ["chromium", "firefox"] if args.browser == "auto" else [args.browser]

    last_err = None
    stats = None
    har_path = None
    for b in browsers:
        try:
            stats, har_path = await run_once(
                browser_name=b,
                url=args.url,
                out_dir=out_dir,
                wait_s=args.wait,
                save_har=args.save_har,
                timeout_ms=args.timeout_ms,
                headed=args.headed,
                tzinfo=tzinfo,
            )
            print("[DONE] capture complete.")
            if cfg.get("telegram", {}).get("enabled"):
                message = format_telegram_message(
                    status="OK",
                    url=args.url,
                    out_dir=out_dir,
                    tz_label=tz_label,
                    tzinfo=tzinfo,
                    stats=stats,
                    har_path=har_path,
                )
                send_telegram_message(cfg, message, logger=logger)
            return
        except PWError as e:
            last_err = e
            print(f"[FAIL] {b} failed: {e}")
        except Exception as e:
            last_err = e
            print(f"[FAIL] {b} failed (non-playwright): {e}")

    if cfg.get("telegram", {}).get("enabled"):
        message = format_telegram_message(
            status="ERROR",
            url=args.url,
            out_dir=out_dir,
            tz_label=tz_label,
            tzinfo=tzinfo,
            stats=stats,
            har_path=har_path,
            error=str(last_err),
        )
        send_telegram_message(cfg, message, logger=logger)
    raise SystemExit(f"All browsers failed. Last error: {last_err}")


def format_telegram_message(
    status: str,
    url: str,
    out_dir: Path,
    tz_label: str,
    tzinfo: tzinfo,
    stats: dict | None,
    har_path: Path | None,
    error: str | None = None,
) -> str:
    if status == "OK":
        head = "✅ <b>CME FedWatch Probabilities</b>"
    else:
        head = "❌ <b>CME FedWatch Probabilities</b>"

    captures = (stats or {}).get("captures", 0)
    json_files = (stats or {}).get("json_files", 0)
    lines = [
        head,
        f"<b>เวลา</b>: {now_local_iso(tzinfo)}",
        f"<b>Timezone</b>: {tz_label}",
        f"<b>URL</b>: {url}",
        f"<b>Output</b>: {out_dir}",
        f"<b>Captures</b>: {captures}",
        f"<b>JSON</b>: {json_files}",
    ]
    if har_path:
        lines.append(f"<b>HAR</b>: {har_path}")
    if error:
        lines.append(f"<b>Error</b>: {error}")
    lines.append("หมายเหตุ: ถ้า Captures = 0 แปลว่าไม่พบ response ที่ต้องการ")
    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(main())
