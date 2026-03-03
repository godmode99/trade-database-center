from __future__ import annotations

import csv
import json
import os
import sys
import getpass
import logging
from enum import Enum
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

BASE_DIR = Path(__file__).resolve().parent
PYTHON_DIR = BASE_DIR.parents[2].resolve()
REPO_ROOT = PYTHON_DIR.parent
TELEGRAM_REPORT_DIR = PYTHON_DIR / "telegram_report"

if TELEGRAM_REPORT_DIR.exists() and str(TELEGRAM_REPORT_DIR) not in sys.path:
    sys.path.insert(0, str(TELEGRAM_REPORT_DIR))

from telegram_notifier import send_telegram_message

DEFAULT_AUTH_URL = "https://login.cmegroup.com/sso/accountstatus/showAuth.action"
DEFAULT_WATCHLIST_URL = "https://www.cmegroup.com/watchlists/details.1769586889025783750.C.html"
DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "Data" / "raw_data" / "cme" / "fedwatch_quotes"
DEFAULT_MAX_EXPIRY_YEAR = 2026
DEFAULT_WATCHLIST_FILTERS = {
    "daily": {"zq": 6, "sr1": 3, "sr3": 3, "zt": 1, "6e": 1},
    "weekly": {"zq": 6, "sr1": 3, "sr3": 3, "zn": 2, "6e": 1, "zt": 1, "zf": 1, "zb": 1},
    "monthly": {"zq": 12, "sr1": 6, "sr3": 6, "zn": 3, "tn": 2, "zb": 2, "ub": 2, "twe": 1, "6e": 1, "e7": 1, "m6e": 1},
}
NAV_TIMEOUT = 60_000
MONTH_ALIASES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

class AuthState(str, Enum):
    AUTHENTICATED = "AUTHENTICATED"
    LOGIN_REQUIRED = "LOGIN_REQUIRED"
    UNAUTHORIZED_OR_EXPIRED = "UNAUTHORIZED_OR_EXPIRED"
    UNKNOWN = "UNKNOWN"

def load_config() -> dict:
    load_env_file(REPO_ROOT)
    cfg_path = Path(__file__).with_name("config.json")
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {}
    return inject_telegram_env(cfg)


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


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("cme_auth_check")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def notify_telegram(cfg: dict, message: str, logger: logging.Logger) -> None:
    if logger:
        logger.info("Telegram notify: %s", message)
    send_telegram_message(cfg, message, logger=logger)
    if logger:
        logger.info("Telegram notify finished")


def queue_telegram(messages: list[str], message: str, logger: logging.Logger | None = None) -> None:
    if logger:
        logger.info("Telegram queue: %s", message)
    messages.append(message)


def format_filtered_counts(counts: dict[str, int]) -> str:
    daily_count = counts.get("daily", 0)
    weekly_count = counts.get("weekly", 0)
    monthly_count = counts.get("monthly", 0)
    return (
        "📊 json preview (array counts)\n"
        f"- daily: {daily_count}\n"
        f"- weekly: {weekly_count}\n"
        f"- monthly: {monthly_count}\n"
        "หมายเหตุ: หากค่าเป็น 0 = scrape ไม่ได้"
    )

def format_watchlist_save_status(counts: dict[str, int]) -> str:
    daily_count = counts.get("daily", 0)
    weekly_count = counts.get("weekly", 0)
    monthly_count = counts.get("monthly", 0)
    return (
        "🗂️ save watchlist files\n"
        f"- daily: save ไฟล์สำเร็จ ({daily_count} rows)\n"
        f"- weekly: save ไฟล์สำเร็จ ({weekly_count} rows)\n"
        f"- monthly: save ไฟล์สำเร็จ ({monthly_count} rows)"
    )

def pick_creds(cfg: dict):
    # 1) config.json
    user = (cfg.get("username") or "").strip()
    pwd  = (cfg.get("password") or "").strip()

    # 2) env fallback
    if not user:
        user = os.environ.get("CME_USER", "").strip()
    if not pwd:
        pwd = os.environ.get("CME_PASS", "").strip()

    # 3) prompt fallback
    if not user:
        user = input("CME username/email: ").strip()
    if not pwd:
        pwd = getpass.getpass("CME password: ").strip()

    return user, pwd

def is_login_page(page) -> bool:
    # จาก HTML ที่มึงแปะมา: #user, #pwd, #loginBtn
    return (
        page.locator("#user").count() > 0
        and page.locator("#pwd").count() > 0
        and page.locator("#loginBtn").count() > 0
    )

def detect_state(page, response_text: str | None = None) -> AuthState:
    text_upper = (response_text or "").upper()
    if "AUTHENTICATED" in text_upper:
        return AuthState.AUTHENTICATED
    if "LOGIN_REQUIRED" in text_upper:
        return AuthState.LOGIN_REQUIRED
    if "UNAUTHORIZED" in text_upper or "EXPIRED" in text_upper:
        return AuthState.UNAUTHORIZED_OR_EXPIRED

    # รอให้หน้า render นิดนึง กัน false positive
    try:
        page.wait_for_function(
            """() => {
                const u = document.querySelector('#user');
                const p = document.querySelector('#pwd');
                const b = document.querySelector('#loginBtn');
                const txt = document.body ? document.body.innerText.toLowerCase() : '';
                return (u && p && b) || txt.includes('session has expired') || txt.includes('unauthorized');
            }""",
            timeout=10_000
        )
    except:
        pass

    if is_login_page(page):
        return AuthState.LOGIN_REQUIRED

    # ถ้าไม่เจอ login form ก็ถือว่า authenticated สำหรับ showAuth URL
    body_txt = ""
    try:
        body_txt = page.locator("body").inner_text(timeout=2000).lower()
    except:
        body_txt = ""

    if "session has expired" in body_txt or "unauthorized" in body_txt:
        return AuthState.UNAUTHORIZED_OR_EXPIRED

    # ยังไงก็ไม่น่า UNKNOWN มาก แต่เผื่อไว้
    return AuthState.AUTHENTICATED

def save_debug(page, prefix="debug"):
    try:
        page.screenshot(path=f"{prefix}.png", full_page=True)
        print(f"📸 saved: {prefix}.png")
    except:
        pass
    try:
        html = page.content()
        with open(f"{prefix}.html", "w", encoding="utf-8") as f:
            f.write(html)
        print(f"🧾 saved: {prefix}.html")
    except:
        pass

def resolve_output_paths(cfg: dict) -> dict[str, Path]:
    output_dir = Path(cfg.get("watchlist_output_dir", DEFAULT_OUTPUT_DIR))
    output_dir.mkdir(parents=True, exist_ok=True)

    nonefilter_dir = output_dir / "nonefilter"
    nonefilter_dir.mkdir(parents=True, exist_ok=True)

    html_output = Path(cfg.get("watchlist_output", nonefilter_dir / "watchlist.html"))
    json_output = Path(cfg.get("watchlist_json_output", nonefilter_dir / "watchlist_filtered.json"))
    csv_output = Path(cfg.get("watchlist_csv_output", nonefilter_dir / "watchlist_filtered.csv"))

    if not html_output.is_absolute():
        html_output = output_dir / html_output
    if not json_output.is_absolute():
        json_output = output_dir / json_output
    if not csv_output.is_absolute():
        csv_output = output_dir / csv_output

    return {
        "output_dir": output_dir,
        "nonefilter_dir": nonefilter_dir,
        "html_output": html_output,
        "json_output": json_output,
        "csv_output": csv_output,
    }

def build_timestamp() -> tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d_%H%M%S"), now.isoformat(timespec="seconds")

def append_timestamp_to_path(path: Path, timestamp: str) -> Path:
    return path.with_name(f"{path.stem}_{timestamp}{path.suffix}")

def add_timestamp_to_payload(payload: list[dict] | list[list[str]], timestamp_iso: str):
    if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
        stamped_payload = []
        for item in payload:
            stamped_item = dict(item)
            stamped_item["timestamp"] = timestamp_iso
            stamped_payload.append(stamped_item)
        return stamped_payload
    return {"timestamp": timestamp_iso, "data": payload}

def fetch_watchlist_html(page, cfg: dict) -> dict[str, str | int] | None:
    watchlist_url = (cfg.get("watchlist_url") or DEFAULT_WATCHLIST_URL).strip()
    outputs = resolve_output_paths(cfg)
    output_path = outputs["html_output"]
    json_output = outputs["json_output"]
    csv_output = outputs["csv_output"]
    timestamp, timestamp_iso = build_timestamp()
    json_output = append_timestamp_to_path(json_output, timestamp)

    try:
        page.goto(watchlist_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        page.wait_for_timeout(1200)
    except PlaywrightTimeoutError:
        print(f"❌ goto watchlist timeout: {watchlist_url}")
        save_debug(page, "watchlist_timeout")
        return None

    table_data = extract_watchlist_table(page)
    payload: list[dict] | list[list[str]] | dict = []
    row_count = 0
    filtered_counts: dict[str, int] = {}
    if table_data is None:
        print("⚠️ watchlist table not found")
    else:
        headers, rows = table_data
        if rows:
            nonefilter_dir = outputs["nonefilter_dir"]
            save_unfiltered_watchlist(headers, rows, nonefilter_dir, timestamp, timestamp_iso)
            max_expiry_year = resolve_max_expiry_year(cfg)
            filtered_rows = filter_watchlist_rows(headers, rows, max_expiry_year=max_expiry_year)
            filtered_headers, filtered_rows = prune_watchlist_columns(
                headers,
                filtered_rows,
                ["Chart URL", "Contract Code", "Product URL"],
            )
            payload = save_table_as_json(filtered_headers, filtered_rows, json_output, timestamp_iso)
            save_table_as_csv(filtered_headers, filtered_rows, csv_output)
            if isinstance(payload, list) and all(isinstance(item, dict) for item in payload):
                filters = resolve_watchlist_filters(cfg)
                filtered_counts = save_filtered_watchlists(
                    payload,
                    outputs["output_dir"],
                    timestamp,
                    timestamp_iso,
                    filters,
                )
            row_count = len(filtered_rows)
        else:
            print("⚠️ watchlist table found but no rows to export")

    try:
        html = page.content()
    except Exception as exc:
        print(f"❌ read watchlist HTML failed: {exc}")
        save_debug(page, "watchlist_read_failed")
        return None

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"✅ saved watchlist html: {output_path}")
    except Exception as exc:
        print(f"❌ write watchlist HTML failed: {exc}")
        save_debug(page, "watchlist_write_failed")
        return None

    return {
        "row_count": row_count,
        "html_output": str(output_path),
        "json_output": str(json_output),
        "csv_output": str(csv_output),
        "filtered_counts": filtered_counts,
    }

def extract_watchlist_table(page) -> tuple[list[str], list[list[str]]] | None:
    selectors = [".watchlist-table", ".watchlist-products table", "table"]
    for selector in selectors:
        try:
            page.wait_for_selector(selector, timeout=10_000)
        except PlaywrightTimeoutError:
            continue
        table_data = page.evaluate(
            """(sel) => {
                const table = document.querySelector(sel);
                if (!table) return null;

                if (table.classList.contains('watchlist-table')) {
                    const headers = [
                        'Name',
                        'Code',
                        'Expiry',
                        'Chart URL',
                        'Last Price',
                        'Change',
                        'High',
                        'Low',
                        'Open',
                        'Volume',
                        'Contract Code',
                        'Front Month',
                        'Product URL',
                    ];

                    const rows = Array.from(table.querySelectorAll('.tbody .tr')).map(row => {
                        const nameCell = row.querySelector('.first-column .table-cell.month-code');
                        let name = '';
                        let code = '';
                        if (nameCell) {
                            const lines = nameCell.innerText
                                .split('\\n')
                                .map(line => line.trim())
                                .filter(Boolean);
                            if (lines.length > 0) name = lines[0];
                            if (lines.length > 1) code = lines[lines.length - 1];
                        }

                        const codeAnchor = row.querySelector('.first-column a.code');
                        if (codeAnchor && codeAnchor.innerText.trim()) {
                            code = codeAnchor.innerText.trim();
                        }
                        const productUrl = codeAnchor ? codeAnchor.href : '';

                        const expiryCell = row.querySelector('.second-column .expiration-month');
                        let expiry = expiryCell ? expiryCell.innerText.trim() : '';
                        if (expiry) {
                            expiry = expiry.replace(/^FM\\b[\\s\\n]*/i, '').trim();
                        }

                        const contractInput = row.querySelector('input[data-contract-code]');
                        const contractCode = contractInput
                            ? contractInput.getAttribute('data-contract-code') || ''
                            : '';
                        const isFrontMonth = contractInput
                            ? (contractInput.getAttribute('data-is-front-month') === 'true')
                            : false;

                        const chartAnchor = row.querySelector('.third-column a[data-code]');
                        const chartUrl = chartAnchor ? chartAnchor.href : '';

                        const valueCells = Array.from(
                            row.querySelectorAll('.third-column .table-cell')
                        ).map(cell => cell.innerText.trim());

                        const lastPrice = valueCells[1] || '';
                        const change = valueCells[2] || '';
                        const high = valueCells[3] || '';
                        const low = valueCells[4] || '';
                        const open = valueCells[5] || '';
                        const volume = valueCells[6] || '';

                        return [
                            name,
                            code,
                            expiry,
                            chartUrl,
                            lastPrice,
                            change,
                            high,
                            low,
                            open,
                            volume,
                            contractCode,
                            isFrontMonth ? 'true' : 'false',
                            productUrl,
                        ];
                    });

                    return { headers, rows };
                }

                const headers = Array.from(table.querySelectorAll('thead th'))
                    .map(th => th.innerText.trim())
                    .filter(Boolean);
                const rows = Array.from(table.querySelectorAll('tbody tr')).map(tr => {
                    return Array.from(tr.querySelectorAll('th, td'))
                        .map(td => td.innerText.trim());
                });
                return { headers, rows };
            }""",
            selector,
        )
        if table_data and table_data.get("rows"):
            headers = table_data.get("headers") or []
            rows = table_data.get("rows") or []
            return headers, rows
    return None

def resolve_max_expiry_year(cfg: dict) -> int | None:
    raw_value = cfg.get(
        "max_expiry_year",
        cfg.get("default_max_expiry_year", cfg.get("DEFAULT_MAX_EXPIRY_YEAR", DEFAULT_MAX_EXPIRY_YEAR)),
    )
    if raw_value in (None, ""):
        return DEFAULT_MAX_EXPIRY_YEAR
    try:
        max_year = int(raw_value)
    except (TypeError, ValueError):
        return DEFAULT_MAX_EXPIRY_YEAR
    if max_year <= 0:
        return None
    return max_year

def parse_expiry_year(expiry: str) -> int | None:
    if not expiry:
        return None
    for token in expiry.replace("/", " ").replace("-", " ").split():
        if token.isdigit() and len(token) == 4:
            return int(token)
    return None

def parse_expiry_month_year(expiry: str) -> tuple[int | None, int | None]:
    if not expiry:
        return None, None
    tokens = expiry.replace("/", " ").replace("-", " ").split()
    month_value = None
    year_value = None
    for token in tokens:
        cleaned = token.strip().upper()
        if cleaned in MONTH_ALIASES:
            month_value = MONTH_ALIASES[cleaned]
            continue
        if cleaned.isdigit():
            if len(cleaned) == 4:
                year_value = int(cleaned)
            else:
                try:
                    numeric = int(cleaned)
                except ValueError:
                    continue
                if 1 <= numeric <= 12:
                    month_value = numeric
    return year_value, month_value

def normalize_expiry_value(expiry: str) -> str:
    if not expiry:
        return expiry
    year, month = parse_expiry_month_year(expiry)
    if year and month:
        return f"{month} {year}"
    return expiry

def expiry_month_distance(expiry: str, now: datetime) -> int | None:
    year, month = parse_expiry_month_year(expiry)
    if not year or not month:
        return None
    return (year - now.year) * 12 + (month - now.month)

def normalize_expiry_in_item(item: dict) -> dict:
    expiry_keys = [key for key in item.keys() if str(key).strip().lower() == "expiry"]
    if not expiry_keys:
        return item
    updated = dict(item)
    for key in expiry_keys:
        updated[key] = normalize_expiry_value(str(item.get(key, "")))
    return updated

def filter_watchlist_rows(
    headers: list[str],
    rows: list[list[str]],
    max_expiry_year: int | None = None,
) -> list[list[str]]:
    if not headers:
        return rows

    header_map = {header.strip().lower(): idx for idx, header in enumerate(headers)}
    last_price_idx = header_map.get("last price")
    volume_idx = header_map.get("volume")
    expiry_idx = header_map.get("expiry")

    has_price_volume = last_price_idx is not None and volume_idx is not None

    filtered_rows = []
    for row in rows:
        if has_price_volume:
            last_price = row[last_price_idx].strip() if last_price_idx < len(row) else ""
            volume = row[volume_idx].strip() if volume_idx < len(row) else ""
            if last_price == "-" and volume == "0":
                continue
        if max_expiry_year and expiry_idx is not None and expiry_idx < len(row):
            expiry_year = parse_expiry_year(row[expiry_idx].strip())
            if expiry_year and expiry_year > max_expiry_year:
                continue
        filtered_rows.append(row)
    return filtered_rows

def prune_watchlist_columns(
    headers: list[str],
    rows: list[list[str]],
    drop_columns: list[str],
) -> tuple[list[str], list[list[str]]]:
    if not headers:
        return headers, rows

    drop_set = {name.strip().lower() for name in drop_columns}
    keep_indices = [idx for idx, header in enumerate(headers) if header.strip().lower() not in drop_set]
    if len(keep_indices) == len(headers):
        return headers, rows

    pruned_headers = [headers[idx] for idx in keep_indices]
    pruned_rows = [
        [row[idx] if idx < len(row) else "" for idx in keep_indices]
        for row in rows
    ]
    return pruned_headers, pruned_rows

def save_table_as_json(
    headers: list[str],
    rows: list[list[str]],
    output_path: Path,
    timestamp_iso: str,
) -> list[dict] | list[list[str]] | dict:
    payload = []
    if headers:
        for row in rows:
            item = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
            payload.append(item)
    else:
        payload = rows

    try:
        payload = add_timestamp_to_payload(payload, timestamp_iso)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"✅ saved watchlist json: {output_path}")
    except Exception as exc:
        print(f"❌ write watchlist json failed: {exc}")
    return payload

def save_table_as_csv(headers: list[str], rows: list[list[str]], output_path: Path) -> None:
    try:
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if headers:
                writer.writerow(headers)
            writer.writerows(rows)
        print(f"✅ saved watchlist csv: {output_path}")
    except Exception as exc:
        print(f"❌ write watchlist csv failed: {exc}")

def save_unfiltered_watchlist(
    headers: list[str],
    rows: list[list[str]],
    output_dir: Path,
    timestamp: str,
    timestamp_iso: str,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_output = append_timestamp_to_path(output_dir / "watchlist_unfiltered.json", timestamp)
    csv_output = output_dir / "watchlist_unfiltered.csv"
    save_table_as_json(headers, rows, json_output, timestamp_iso)
    save_table_as_csv(headers, rows, csv_output)
    return {"json_output": json_output, "csv_output": csv_output}

def extract_code_from_item(item: dict) -> str:
    for key in ("Code", "code", "Contract Code", "contract_code", "contractCode"):
        value = item.get(key)
        if value:
            return str(value).strip()
    return ""

def filter_watchlist_by_prefix_limits(
    payload: list[dict],
    prefixes: dict[str, int | None],
    now: datetime,
) -> list[dict]:
    selected: list[dict] = []
    seen_keys: set[tuple[tuple[str, str], ...]] = set()

    for prefix, limit in prefixes.items():
        normalized_prefix = prefix.lower()
        candidates = []
        for item in payload:
            code = extract_code_from_item(item).lower()
            if not code or not code.startswith(normalized_prefix):
                continue
            distance = expiry_month_distance(str(item.get("Expiry", "")), now)
            if distance is None:
                distance_key = (1, 9999)
            else:
                distance_key = (0 if distance >= 0 else 1, abs(distance))
            candidates.append((distance_key, item))

        candidates.sort(key=lambda entry: entry[0])
        limit_count = int(limit) if limit is not None else None
        if limit_count is not None and limit_count < 0:
            limit_count = 0
        count = 0
        for _, item in candidates:
            key_items = []
            for k, v in item.items():
                if k == "Front Month":
                    continue
                key_items.append((str(k), "" if v is None else str(v)))
            key = tuple(sorted(key_items))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            selected.append(item)
            count += 1
            if limit_count is not None and count >= limit_count:
                break

    return selected

def normalize_front_month(value) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"

def resolve_watchlist_filters(cfg: dict) -> dict[str, dict[str, int | None]]:
    cfg_filters = cfg.get("watchlist_filters") or {}
    resolved: dict[str, dict[str, int | None]] = {}
    for bucket, prefixes in DEFAULT_WATCHLIST_FILTERS.items():
        cfg_value = cfg_filters.get(bucket)
        if isinstance(cfg_value, dict):
            cleaned: dict[str, int | None] = {}
            for prefix, limit in cfg_value.items():
                prefix_str = str(prefix).strip().lower()
                if not prefix_str:
                    continue
                try:
                    limit_value = int(limit) if limit is not None else None
                except (TypeError, ValueError):
                    limit_value = None
                cleaned[prefix_str] = limit_value
            resolved[bucket] = cleaned
        elif isinstance(cfg_value, list):
            cleaned = {
                str(prefix).strip().lower(): None
                for prefix in cfg_value
                if str(prefix).strip()
            }
            resolved[bucket] = cleaned
        else:
            resolved[bucket] = dict(prefixes)
    extra_filters = {
        key: value
        for key, value in cfg_filters.items()
        if key not in resolved and isinstance(value, (list, dict))
    }
    for bucket, prefixes in extra_filters.items():
        if isinstance(prefixes, dict):
            resolved[bucket] = {
                str(prefix).strip().lower(): (int(limit) if limit is not None else None)
                for prefix, limit in prefixes.items()
                if str(prefix).strip()
            }
        else:
            resolved[bucket] = {
                str(prefix).strip().lower(): None
                for prefix in prefixes
                if str(prefix).strip()
            }
    return resolved

def drop_false_front_month_duplicates(payload: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: dict[tuple[tuple[str, str], ...], int] = {}
    for item in payload:
        key_items = []
        for k, v in item.items():
            if k == "Front Month":
                continue
            key_items.append((str(k), "" if v is None else str(v)))
        key = tuple(sorted(key_items))
        existing_idx = seen.get(key)
        if existing_idx is None:
            seen[key] = len(deduped)
            deduped.append(item)
            continue
        if not normalize_front_month(deduped[existing_idx].get("Front Month")) and normalize_front_month(
            item.get("Front Month")
        ):
            deduped[existing_idx] = item
    return deduped

def save_filtered_watchlists(
    payload: list[dict],
    output_dir: Path,
    timestamp: str,
    timestamp_iso: str,
    filters: dict[str, dict[str, int | None]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    now = datetime.now()

    for bucket, prefixes in filters.items():
        bucket_dir = output_dir / bucket
        bucket_dir.mkdir(parents=True, exist_ok=True)
        filtered_payload = filter_watchlist_by_prefix_limits(payload, prefixes, now)
        filtered_payload = drop_false_front_month_duplicates(filtered_payload)
        filtered_payload = [normalize_expiry_in_item(item) for item in filtered_payload]
        counts[bucket] = len(filtered_payload)
        output_path = append_timestamp_to_path(bucket_dir / "watchlist.json", timestamp)
        try:
            filtered_payload = add_timestamp_to_payload(filtered_payload, timestamp_iso)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(filtered_payload, f, ensure_ascii=False, indent=2)
            print(f"✅ saved {bucket} watchlist json: {output_path}")
        except Exception as exc:
            print(f"❌ write {bucket} watchlist json failed: {exc}")
    return counts

def main():
    cfg = load_config()
    logger = setup_logger()
    messages: list[str] = []

    auth_url = (cfg.get("auth_url") or DEFAULT_AUTH_URL).strip()
    user_data_dir = (cfg.get("user_data_dir") or os.environ.get("CME_USER_DATA_DIR") or "cme_profile").strip()

    def run() -> int:
        exit_code = 0
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir,
                headless=False,
            )
            page = context.new_page()
            try:
                # 1) เริ่มที่ auth_url เสมอ
                try:
                    response = page.goto(auth_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                    page.wait_for_timeout(1200)
                except PlaywrightTimeoutError:
                    print("❌ goto auth_url timeout")
                    save_debug(page, "auth_timeout")
                    queue_telegram(messages, "❌ CME auth check: auth_url timeout", logger)
                    return 1

                response_text = None
                if response is not None:
                    try:
                        response_text = response.text()
                    except Exception:
                        response_text = None
                state = detect_state(page, response_text=response_text)
                print(f"STATE: {state} | url={page.url}")
                queue_telegram(
                    messages,
                    (
                        "🔐 CME auth check\n"
                        f"- state: {state}"
                    ),
                    logger,
                )

                if state == AuthState.AUTHENTICATED:
                    print("✅ Already logged in")
                    queue_telegram(messages, "✅ CME auth check: already logged in", logger)
                    watchlist_summary = fetch_watchlist_html(page, cfg)
                    if watchlist_summary:
                        queue_telegram(
                            messages,
                            (
                                "📄 CME watchlist export (authenticated)\n"
                                f"- rows: {watchlist_summary['row_count']}\n"
                                f"{format_watchlist_save_status(watchlist_summary['filtered_counts'])}"
                            ),
                            logger,
                        )
                    return exit_code

                # 2) ต้อง login
                print("⚠️ Need login -> จะพยายามกรอกให้")
                queue_telegram(
                    messages,
                    (
                        "⚠️ CME auth check: login required\n"
                        "- action: attempting auto login"
                    ),
                    logger,
                )
                user, pwd = pick_creds(cfg)

                try:
                    page.wait_for_selector("#user", timeout=20_000)
                    page.wait_for_selector("#pwd", timeout=20_000)
                    page.wait_for_selector("#loginBtn", timeout=20_000)

                    page.fill("#user", user)
                    page.fill("#pwd", pwd)
                    page.click("#loginBtn")

                    # อาจติด reCAPTCHA/MFA -> ให้ทำเองได้
                    try:
                        page.wait_for_load_state("networkidle", timeout=30_000)
                    except:
                        pass

                except Exception as e:
                    print(f"❌ Error while filling login: {e}")

                # 3) เช็คซ้ำด้วย auth_url
                response = page.goto(auth_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                page.wait_for_timeout(1200)
                response_text = None
                if response is not None:
                    try:
                        response_text = response.text()
                    except Exception:
                        response_text = None
                state2 = detect_state(page, response_text=response_text)
                print(f"AFTER LOGIN STATE: {state2} | url={page.url}")
                queue_telegram(
                    messages,
                    (
                        "🔐 CME auth check after login\n"
                        f"- state: {state2}"
                    ),
                    logger,
                )

                if state2 == AuthState.AUTHENTICATED:
                    print("✅ Login success")
                    queue_telegram(messages, "✅ CME auth check: login success", logger)
                    watchlist_summary = fetch_watchlist_html(page, cfg)
                    if watchlist_summary:
                        queue_telegram(
                            messages,
                            (
                                "📄 CME watchlist export (auto login)\n"
                                f"- rows: {watchlist_summary['row_count']}\n"
                                f"- json: {watchlist_summary['json_output']}\n"
                                f"- csv: {watchlist_summary['csv_output']}\n"
                                f"- html: {watchlist_summary['html_output']}\n"
                                f"{format_filtered_counts(watchlist_summary['filtered_counts'])}"
                            ),
                            logger,
                        )
                    return exit_code

                print("❌ ยังไม่สำเร็จ (อาจติด reCAPTCHA/MFA/OTP หรือรหัสผิด)")
                print("➡️ ไปทำขั้นตอนบน browser ให้ผ่าน แล้วกลับมากด Enter เพื่อเช็คซ้ำ")
                input()

                response = page.goto(auth_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
                page.wait_for_timeout(1200)
                response_text = None
                if response is not None:
                    try:
                        response_text = response.text()
                    except Exception:
                        response_text = None
                state3 = detect_state(page, response_text=response_text)
                print(f"AFTER MANUAL STATE: {state3} | url={page.url}")
                queue_telegram(
                    messages,
                    (
                        "🔐 CME auth check after manual\n"
                        f"- state: {state3}"
                    ),
                    logger,
                )

                if state3 == AuthState.AUTHENTICATED:
                    print("✅ Success after manual")
                    queue_telegram(messages, "✅ CME auth check: success after manual step", logger)
                    watchlist_summary = fetch_watchlist_html(page, cfg)
                    if watchlist_summary:
                        queue_telegram(
                            messages,
                            (
                                "📄 CME watchlist export (manual)\n"
                                f"- rows: {watchlist_summary['row_count']}\n"
                                f"- json: {watchlist_summary['json_output']}\n"
                                f"- csv: {watchlist_summary['csv_output']}\n"
                                f"- html: {watchlist_summary['html_output']}\n"
                                f"{format_filtered_counts(watchlist_summary['filtered_counts'])}"
                            ),
                            logger,
                        )
                    return exit_code

                save_debug(page, "auth_failed")
                queue_telegram(messages, "❌ CME auth check: authentication failed", logger)
                return 2
            finally:
                context.close()

        return exit_code

    exit_code = run()
    if messages:
        notify_telegram(cfg, "\n\n".join(messages), logger)
    if exit_code:
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
