from __future__ import annotations

import sys
from pathlib import Path

# -------------------------------------------------------------------
# run_fetch.py location: <repo_root>/python/fetch/mt5/run_fetch.py
# telegram_notifier.py location: <repo_root>/python/telegram_report/telegram_notifier.py
# -------------------------------------------------------------------
BASE_DIR = Path(__file__).parent.resolve()          # .../python/fetch/mt5
PYTHON_DIR = BASE_DIR.parents[1].resolve()         # .../python
TELEGRAM_REPORT_DIR = PYTHON_DIR / "telegram_report"

# Add telegram_report directory to import path
if TELEGRAM_REPORT_DIR.exists() and str(TELEGRAM_REPORT_DIR) not in sys.path:
    sys.path.insert(0, str(TELEGRAM_REPORT_DIR))

# Fail fast with clear error if path wrong
if not (TELEGRAM_REPORT_DIR / "telegram_notifier.py").exists():
    raise FileNotFoundError(
        f"telegram_notifier.py not found at: {TELEGRAM_REPORT_DIR / 'telegram_notifier.py'}\n"
        f"BASE_DIR={BASE_DIR}\n"
        f"PYTHON_DIR={PYTHON_DIR}\n"
        f"TELEGRAM_REPORT_DIR={TELEGRAM_REPORT_DIR}"
    )

from telegram_notifier import (
    send_telegram_message,
    format_manifest_message,
    classify_manifest,
)

from utils import load_config, setup_logger
from pipeline import run_fetch_pipeline


def run_with_config(config_filename: str) -> None:
    cfg_path = BASE_DIR / config_filename
    if not cfg_path.exists():
        cfg_path = BASE_DIR / "app" / config_filename
    cfg = load_config(str(cfg_path))

    logs_dir = (BASE_DIR / cfg["output"]["logs_dir"]).resolve()
    logger = setup_logger(logs_dir, name="fetch")

    logger.info("=== FETCH PIPELINE START ===")
    manifest = run_fetch_pipeline(cfg, logger, base_dir=BASE_DIR)
    logger.info(f"Manifest summary: stale_sources={manifest.get('stale_sources', [])}")
    logger.info("=== FETCH PIPELINE END ===")

    tg = cfg.get("telegram", {}) or {}
    status = classify_manifest(manifest)

    send_ok = bool(tg.get("send_on_success", True))
    send_warn = bool(tg.get("send_on_warning", True))
    send_err = bool(tg.get("send_on_error", True))

    should_send = (status == "OK" and send_ok) or (status == "WARN" and send_warn) or (status == "ERROR" and send_err)
    if should_send:
        msg = format_manifest_message(manifest)
        send_telegram_message(cfg, msg, logger=logger)
