from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).parent.resolve()            # .../python/fetch/fred
PYTHON_DIR = BASE_DIR.parents[1].resolve()           # .../python
TELEGRAM_REPORT_DIR = PYTHON_DIR / "telegram_report"

if TELEGRAM_REPORT_DIR.exists() and str(TELEGRAM_REPORT_DIR) not in sys.path:
    sys.path.insert(0, str(TELEGRAM_REPORT_DIR))

from telegram_notifier import (
    send_telegram_message,
    format_manifest_message,
    classify_manifest,
)

from utils import load_config, setup_logger
from pipeline import run_fetch_pipeline


def _title_modes(modes: Iterable[str]) -> str:
    return "/".join(mode.strip().title() for mode in modes if mode and str(mode).strip())


def _resolve_mode_label(cfg: dict, mode_label: str | None) -> str:
    if mode_label:
        return mode_label.strip()
    fred = cfg.get("fred", {}) or {}
    run_modes = fred.get("run_modes")
    if run_modes is None:
        run_modes = [fred.get("run_mode", "")]
    if isinstance(run_modes, str):
        run_modes = [run_modes]
    label = _title_modes(run_modes)
    return label


def run_with_config(config_filename: str, mode_label: str | None = None) -> None:
    cfg_path = BASE_DIR / config_filename
    cfg = load_config(str(cfg_path))

    logs_dir = (BASE_DIR / cfg["output"]["logs_dir"]).resolve()
    logger = setup_logger(logs_dir, name="fetch_fred")

    logger.info("=== FRED FETCH PIPELINE START ===")
    manifest = run_fetch_pipeline(cfg, logger, base_dir=BASE_DIR)
    logger.info("=== FRED FETCH PIPELINE END ===")

    tg = cfg.get("telegram", {}) or {}
    status = classify_manifest(manifest)

    send_ok = bool(tg.get("send_on_success", True))
    send_warn = bool(tg.get("send_on_warning", True))
    send_err = bool(tg.get("send_on_error", True))

    should_send = (status == "OK" and send_ok) or (status == "WARN" and send_warn) or (status == "ERROR" and send_err)
    if should_send:
        label = _resolve_mode_label(cfg, mode_label)
        prefix = "FRED" if not label else f"FRED {label}"
        msg = format_manifest_message(manifest).replace("MT5 Fetch", prefix)
        send_telegram_message(cfg, msg, logger=logger)
