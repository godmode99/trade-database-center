# sofr_probabilities_main.py
# CME SOFRWatch Sniffer (Playwright Sync)
#
# Install:
#   pip install playwright
#   playwright install
#
# Run:
#   python sofr_probabilities_main.py --browser auto --wait_s 25
#   python sofr_probabilities_main.py --browser firefox --wait_s 25
#   python sofr_probabilities_main.py --browser chromium --channel chrome --wait_s 25
#   python sofr_probabilities_main.py --browser chromium --headless false --wait_s 25
#   python sofr_probabilities_main.py --browser auto --strict_filter --json_only

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import logging
import json
import os
import re
import time
from urllib.parse import urlparse
from dataclasses import dataclass, field
from html import escape as html_escape, unescape
from pathlib import Path
from typing import Optional, Tuple

from playwright.sync_api import sync_playwright

TARGET_URL = "https://www.cmegroup.com/markets/interest-rates/cme-sofrwatch.html"
DEFAULT_OUTDIR = Path("python/Data/raw_data/cme/fedwatch_probabilities/sofr")

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

BLOCK_RESOURCE_TYPES = {"image", "media", "font"}

# กรอง URL ที่ “น่าจะเป็น XHR/JSON/API” (ปรับได้)
INTERESTING_URL_RE = re.compile(
    r"(sofrwatch|sofr|fedwatch|fed|fomc|watch|prob|probab|dataservice|api|graphql|xhr|json|rates)",
    re.IGNORECASE,
)

# เก็บเฉพาะ HTML ที่มี keyword สำคัญ (SOFRWatch payload ที่ต้องการ)
REQUIRED_HTML_KEYWORDS = (
    'id="doc3"',
    '<div id="doc3" class="do-mobile">',
)

# network error ที่ควร fallback
FATAL_NAV_SIGNS = (
    "ERR_HTTP2_PROTOCOL_ERROR",
    "net::ERR_HTTP2_PROTOCOL_ERROR",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_TIMED_OUT",
    "ERR_NAME_NOT_RESOLVED",
    "ERR_SSL_PROTOCOL_ERROR",
    "ERR_CERT",
)

BASE_DIR = Path(__file__).resolve().parent
PYTHON_DIR = BASE_DIR.parents[2].resolve()
REPO_ROOT = PYTHON_DIR.parent
TELEGRAM_REPORT_DIR = PYTHON_DIR / "telegram_report"
DEFAULT_CONFIG_PATH = BASE_DIR / "probabilities_config.json"

if TELEGRAM_REPORT_DIR.exists() and str(TELEGRAM_REPORT_DIR) not in os.sys.path:
    os.sys.path.insert(0, str(TELEGRAM_REPORT_DIR))

from telegram_notifier import send_telegram_message


@dataclass
class RunConfig:
    browser: str                  # auto|chromium|firefox|webkit
    channel: Optional[str]        # chrome|msedge (chromium only)
    headless: bool
    wait_s: float
    outdir: Path
    save_har: bool
    ua: str
    strict_filter: bool
    timeout_ms: int
    json_only: bool
    telegram_cfg: dict


@dataclass
class StepStatus:
    name: str
    ok: bool
    detail: Optional[str] = None


@dataclass
class TableSummary:
    rows: int
    sample: Optional[dict[str, Optional[float] | str]] = None


@dataclass
class RunSummary:
    steps: list[StepStatus] = field(default_factory=list)
    parsed_tables: dict[str, TableSummary] = field(default_factory=dict)
    json_saved: int = 0
    parsed_json_saved: int = 0
    raw_saved: int = 0
    engine_used: Optional[str] = None

    def add_step(self, name: str, ok: bool, detail: Optional[str] = None) -> None:
        self.steps.append(StepStatus(name=name, ok=ok, detail=detail))

    def record_json_saved(self) -> None:
        self.json_saved += 1

    def record_parsed_json(self, tables: dict[str, list[dict[str, Optional[float]]]]) -> None:
        self.parsed_json_saved += 1
        for table_name, rows in tables.items():
            sample = rows[0] if rows else None
            if table_name in self.parsed_tables:
                self.parsed_tables[table_name].rows += len(rows)
                if not self.parsed_tables[table_name].sample and sample:
                    self.parsed_tables[table_name].sample = sample
            else:
                self.parsed_tables[table_name] = TableSummary(rows=len(rows), sample=sample)

    def record_raw_saved(self) -> None:
        self.raw_saved += 1


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("sofr_probabilities")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


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
    cfg = {"telegram": {"enabled": True}}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        cfg.update(loaded or {})
        if "telegram" in loaded:
            cfg["telegram"] = loaded.get("telegram") or cfg.get("telegram", {})
    return inject_telegram_env(cfg)


def notify_telegram(cfg: RunConfig, message: str, logger: logging.Logger) -> None:
    logger.info("Telegram notify: %s", message)
    send_telegram_message(cfg.telegram_cfg, message, logger=logger)
    logger.info("Telegram notify finished")


def format_telegram_message(
    summary: RunSummary,
    cfg: RunConfig,
    ok: bool,
    error: Optional[str] = None,
) -> str:
    status_icon = "✅" if ok else "❌"
    engine = summary.engine_used or cfg.browser
    lines = [
        f"{status_icon} <b>SOFRZQ Fed Watch Probabilities</b>",
        f"<b>engine</b>: {html_escape(str(engine))}",
        f"<b>time_utc</b>: {datetime.now(timezone.utc).isoformat()}",
        f"<b>outdir</b>: {html_escape(str(cfg.outdir))}",
    ]

    if summary.steps:
        lines.append("<b>steps</b>:")
        for step in summary.steps:
            icon = "✅" if step.ok else "❌"
            detail = f" - {html_escape(step.detail)}" if step.detail else ""
            lines.append(f"• {icon} {html_escape(step.name)}{detail}")

    lines.append("<b>json_summary</b>:")
    lines.append(
        f"• parsed_tables={len(summary.parsed_tables)}, "
        f"parsed_json_saved={summary.parsed_json_saved}, "
        f"json_saved={summary.json_saved}, raw_saved={summary.raw_saved}"
    )
    if summary.parsed_tables:
        lines.append("• fields: symbol, contract_month, prediction, current, diff")
        for table_name, info in summary.parsed_tables.items():
            line = f"• {html_escape(table_name)}: rows={info.rows}"
            if info.sample:
                sample = info.sample
                sample_bits = []
                for key in ("symbol", "contract_month", "prediction", "current", "diff"):
                    if key in sample and sample[key] is not None:
                        sample_bits.append(f"{key}={sample[key]}")
                if sample_bits:
                    line += f" (sample: {', '.join(sample_bits)})"
            lines.append(line)

    if error:
        lines.append(f"<b>error</b>: {html_escape(error)}")

    return "\n".join(lines)


def ensure_outdir(outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "responses").mkdir(parents=True, exist_ok=True)
    (outdir / "meta").mkdir(parents=True, exist_ok=True)
    (outdir / "json").mkdir(parents=True, exist_ok=True)


def safe_filename_from_url(url: str, max_len: int = 120) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]+", "_", url)
    if max_len < 1:
        max_len = 1
    if len(clean) > max_len:
        clean = clean[:max_len]
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{clean}__{h}"


def build_output_stem(url: str, ts: str, max_len: int = 120) -> str:
    parsed = urlparse(url)
    host = parsed.hostname or "unknown_host"
    prefix = f"sofrwatch_{host}_{ts}__"
    slug = safe_filename_from_url(url, max_len=max_len)
    if len(prefix) + len(slug) > max_len:
        max_slug_len = max_len - len(prefix)
        slug = safe_filename_from_url(url, max_len=max_slug_len)
        if len(prefix) + len(slug) > max_len:
            slug = hashlib.sha256(url.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}{slug}"


def strip_tags(raw: str) -> str:
    cleaned = re.sub(r"<[^>]+>", "", raw)
    cleaned = unescape(cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def parse_number(raw: str) -> Optional[float]:
    cleaned = strip_tags(raw).replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_sofr_tables(html_text: str) -> dict[str, list[dict[str, Optional[float]]]]:
    tables: dict[str, list[dict[str, Optional[float]]]] = {}
    table_blocks = re.findall(r"<table[^>]*class=\"grid-thm[^>]*>.*?</table>", html_text, re.DOTALL | re.IGNORECASE)
    for table_html in table_blocks:
        header_match = re.search(r"<th[^>]*colspan=[\"']?5[\"']?>(.*?)</th>", table_html, re.DOTALL | re.IGNORECASE)
        if not header_match:
            continue
        table_name = strip_tags(header_match.group(1))
        if not table_name:
            continue

        rows: list[dict[str, Optional[float]]] = []
        for row_html in re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE):
            if re.search(r"<th", row_html, re.IGNORECASE):
                continue
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL | re.IGNORECASE)
            if len(cells) < 5:
                continue
            symbol = strip_tags(cells[0])
            contract_month = strip_tags(cells[1])
            if not symbol or not contract_month:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "contract_month": contract_month,
                    "prediction": parse_number(cells[2]),
                    "current": parse_number(cells[3]),
                    "diff": parse_number(cells[4]),
                }
            )
        if rows:
            tables[table_name] = rows
    return tables


def is_fatal_nav_error(e: Exception) -> bool:
    msg = str(e)
    return any(sig in msg for sig in FATAL_NAV_SIGNS)


def route_filter(route) -> None:
    req = route.request
    if req.resource_type in BLOCK_RESOURCE_TYPES:
        return route.abort()
    return route.continue_()


def dump_response(
    cfg: RunConfig,
    summary: RunSummary,
    url: str,
    status: int,
    headers: dict,
    body_bytes: bytes,
) -> None:
    ctype = (headers.get("content-type") or headers.get("Content-Type") or "").lower()

    if 300 <= status < 400:
        return

    # strict filter = เซฟเฉพาะ URL ที่ดูเข้าข่ายข้อมูลสำคัญ
    if cfg.strict_filter and not INTERESTING_URL_RE.search(url):
        return

    # กันไฟล์บวม
    if len(body_bytes) > 15 * 1024 * 1024:
        return

    html_text: Optional[str] = None
    if "text/html" not in ctype:
        return

    html_text = body_bytes.decode("utf-8", errors="replace")
    if not all(keyword in html_text for keyword in REQUIRED_HTML_KEYWORDS):
        return

    ts = time.strftime("%Y%m%d_%H%M%S")
    base = build_output_stem(url, ts)

    meta_path = cfg.outdir / "meta" / f"{ts}__{base}.json"
    meta = {
        "timestamp": ts,
        "url": url,
        "status": status,
        "content_type": ctype,
        "headers": {k: v for k, v in headers.items()},
        "size_bytes": len(body_bytes),
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # ถ้าอยากเก็บ “แต่ JSON” เพียวๆ
    if cfg.json_only:
        if ("application/json" not in ctype) and (not ctype.endswith("+json")):
            return

    resp_dir = cfg.outdir / "responses"

    # JSON pretty
    if "application/json" in ctype or ctype.endswith("+json"):
        try:
            data = json.loads(body_bytes.decode("utf-8", errors="replace"))
            out_path = resp_dir / f"{ts}__{base}.json"
            out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[SAVE][json] {status} {url} -> {out_path.name}")
            summary.record_json_saved()
            return
        except Exception:
            # ถ้า decode/parse ไม่ได้ ก็ไหลไปแบบ text/binary
            pass

    # text-ish vs binary
    is_texty = any(x in ctype for x in ["text/", "application/javascript", "application/xml", "text/html"])
    if is_texty:
        out_path = resp_dir / f"{ts}__{base}.txt"
        out_path.write_text(html_text or body_bytes.decode("utf-8", errors="replace"), encoding="utf-8")
        print(f"[SAVE][txt] {status} {url} -> {out_path.name}")
        summary.record_raw_saved()
        if html_text:
            tables = parse_sofr_tables(html_text)
            if tables:
                json_out = cfg.outdir / "json" / f"{ts}__{base}.json"
                payload = {
                    "source_file": out_path.name,
                    "parsed_at_utc": datetime.now(timezone.utc).isoformat(),
                    "tables": tables,
                }
                json_out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                print(f"[SAVE][parsed-json] {status} {url} -> {json_out.name}")
                summary.record_parsed_json(tables)
    else:
        out_path = resp_dir / f"{ts}__{base}.bin"
        out_path.write_bytes(body_bytes)
        print(f"[SAVE][bin] {status} {url} -> {out_path.name}")
        summary.record_raw_saved()


def goto_with_fallback(page, url: str, timeout_ms: int) -> None:
    # domcontentloaded -> load -> plain
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        return
    except Exception as e1:
        print(f"[WARN] goto(domcontentloaded) failed: {e1}")

    try:
        page.goto(url, wait_until="load", timeout=timeout_ms)
        return
    except Exception as e2:
        print(f"[WARN] goto(load) failed: {e2}")

    page.goto(url, timeout=timeout_ms)


def launch_browser(p, browser_name: str, headless: bool, channel: Optional[str]):
    if browser_name == "chromium":
        if channel:
            return p.chromium.launch(headless=headless, channel=channel)
        return p.chromium.launch(
            headless=headless,
            args=[
                "--disable-quic",
                "--disable-blink-features=AutomationControlled",
            ],
        )
    if browser_name == "firefox":
        return p.firefox.launch(headless=headless)
    if browser_name == "webkit":
        return p.webkit.launch(headless=headless)
    raise ValueError(f"Unknown browser: {browser_name}")


def build_context(browser, cfg: RunConfig):
    context_kwargs = dict(
        user_agent=cfg.ua,
        ignore_https_errors=True,
        locale="en-US",
        timezone_id="America/New_York",
    )

    if cfg.save_har:
        har_path = cfg.outdir / "sofrwatch.har"
        context_kwargs.update(
            record_har_path=str(har_path),
            record_har_content="embed",
            record_har_mode="full",
        )
        print(f"[INFO] HAR enabled: {har_path.name}")

    context = browser.new_context(**context_kwargs)

    # ลด noise
    context.route("**/*", route_filter)

    return context


def attach_sniffer(page, cfg: RunConfig, summary: RunSummary):
    def on_response(resp):
        try:
            if 300 <= resp.status < 400:
                return
            url = resp.url
            status = resp.status
            headers = resp.headers
            body = resp.body()
            dump_response(cfg, summary, url, status, headers, body)
        except Exception as e:
            print(f"[WARN] on_response error: {e}")

    page.on("response", on_response)


def try_open_with_engine(
    p,
    engine: str,
    cfg: RunConfig,
    summary: RunSummary,
    logger: logging.Logger,
) -> Tuple[bool, str]:
    """
    returns (ok, used_engine)
    """
    browser = None
    context = None
    page = None

    try:
        browser = launch_browser(p, engine, cfg.headless, cfg.channel if engine == "chromium" else None)
        context = build_context(browser, cfg)
        page = context.new_page()
        attach_sniffer(page, cfg, summary)

        print(f"[INFO] Opening: {TARGET_URL}")
        goto_with_fallback(page, TARGET_URL, timeout_ms=cfg.timeout_ms)
        summary.add_step(f"open page ({engine})", True)

        # รอ XHR/fetch ยิงข้อมูล
        print(f"[INFO] Waiting {cfg.wait_s:.1f}s for XHR/fetch...")
        page.wait_for_timeout(int(cfg.wait_s * 1000))
        summary.add_step(f"wait {cfg.wait_s:.1f}s for XHR/fetch", True)
        summary.engine_used = engine

        return True, engine

    except Exception as e:
        print(f"[WARN] Navigation failed on {engine}: {e}")
        summary.add_step(f"open page ({engine})", False, detail=str(e))
        logger.warning("Navigation failed on %s: %s", engine, e)
        # ถ้าเป็น error แบบ network/protocol ให้ caller ตัดสินใจ fallback
        if not is_fatal_nav_error(e):
            # ไม่ใช่ error กลุ่มที่เราตั้งใจ fallback ก็โยนต่อ (ให้รู้ว่าพังจริง)
            raise
        return False, engine

    finally:
        try:
            if context:
                context.close()
        except:
            pass
        try:
            if browser:
                browser.close()
        except:
            pass


def run(cfg: RunConfig, summary: RunSummary, logger: logging.Logger) -> int:
    ensure_outdir(cfg.outdir)
    summary.add_step("prepare output dir", True, detail=str(cfg.outdir))

    with sync_playwright() as p:
        used = cfg.browser

        # --- AUTO: chromium -> firefox -> webkit (optional) ---
        if cfg.browser == "auto":
            # 1) chromium
            ok, used = try_open_with_engine(p, "chromium", cfg, summary, logger)
            if ok:
                print(f"[DONE] Finished with {used}.")
                return 0

            print("[INFO] Fallback to firefox...")
            ok, used = try_open_with_engine(p, "firefox", cfg, summary, logger)
            if ok:
                print(f"[DONE] Finished with {used}.")
                return 0

            # ถ้าจะสุดทางค่อย webkit
            print("[INFO] Fallback to webkit...")
            ok, used = try_open_with_engine(p, "webkit", cfg, summary, logger)
            if ok:
                print(f"[DONE] Finished with {used}.")
                return 0

            raise RuntimeError("All engines failed (chromium/firefox/webkit).")

        # --- Specific engine ---
        ok, used = try_open_with_engine(p, cfg.browser, cfg, summary, logger)
        if not ok:
            raise RuntimeError(f"Failed to open with {used}. Try --browser firefox or --headless false.")
        print(f"[DONE] Finished with {used}.")
        return 0


def parse_args() -> RunConfig:
    ap = argparse.ArgumentParser(description="CME SOFRWatch Playwright response sniffer")
    ap.add_argument("--browser", choices=["auto", "chromium", "firefox", "webkit"], default="auto")
    ap.add_argument("--channel", choices=["chrome", "msedge"], default=None,
                    help="Chromium channel (use installed Chrome/Edge). Works only with --browser chromium/auto.")
    ap.add_argument("--headless", type=str, default="true", help="true/false")
    ap.add_argument("--wait_s", type=float, default=20.0)
    ap.add_argument("--outdir", type=str, default=str(DEFAULT_OUTDIR))
    ap.add_argument("--save_har", action="store_true")
    ap.add_argument("--ua", type=str, default=DEFAULT_UA)
    ap.add_argument("--strict_filter", action="store_true")
    ap.add_argument("--timeout_ms", type=int, default=60000)
    ap.add_argument("--json_only", action="store_true", help="Save only JSON responses (plus meta)")
    ap.add_argument("--config", type=str, default=str(DEFAULT_CONFIG_PATH))

    ns = ap.parse_args()
    headless = str(ns.headless).strip().lower() in {"1", "true", "yes", "y"}
    telegram_cfg = load_config(Path(ns.config))

    return RunConfig(
        browser=ns.browser,
        channel=ns.channel,
        headless=headless,
        wait_s=ns.wait_s,
        outdir=Path(ns.outdir),
        save_har=ns.save_har,
        ua=ns.ua,
        strict_filter=ns.strict_filter,
        timeout_ms=ns.timeout_ms,
        json_only=ns.json_only,
        telegram_cfg=telegram_cfg,
    )


if __name__ == "__main__":
    cfg = parse_args()
    logger = setup_logger()
    summary = RunSummary()
    print(f"[INFO] Browser: {cfg.browser} | channel={cfg.channel} | headless={cfg.headless}")
    print(f"[INFO] Outdir: {cfg.outdir.resolve()}")
    error: Optional[str] = None
    ok = True
    exit_code = 1
    try:
        exit_code = run(cfg, summary, logger)
    except Exception as exc:
        ok = False
        error = str(exc)
        logger.exception("SOFRWatch run failed: %s", exc)
    finally:
        message = format_telegram_message(summary, cfg, ok=ok, error=error)
        notify_telegram(cfg, message, logger)
    raise SystemExit(exit_code)