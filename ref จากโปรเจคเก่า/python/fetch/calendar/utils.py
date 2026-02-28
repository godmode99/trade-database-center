from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import yaml


def load_config(path: str) -> Dict[str, Any]:
    load_env_file(Path(path).resolve().parent)
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    return apply_env_overrides(cfg)


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


def apply_env_overrides(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not cfg:
        return cfg
    telegram = cfg.get("telegram", {}) or {}

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if tg_token:
        telegram["bot_token"] = tg_token

    tg_chat = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if tg_chat:
        telegram["chat_id"] = tg_chat

    cfg["telegram"] = telegram
    return cfg


def ensure_dir(p: str | Path) -> Path:
    pp = Path(p).resolve()
    pp.mkdir(parents=True, exist_ok=True)
    return pp


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    atomic_write_text(path, text)


def setup_logger(logs_dir: Path, name: str = "fetch_calendar") -> logging.Logger:
    ensure_dir(logs_dir)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = logs_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.INFO)

    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def retry(fn, attempts: int, sleep_seconds: int, logger: logging.Logger, label: str):
    last_err = None
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            last_err = e
            logger.warning(f"{label}: attempt {i}/{attempts} failed: {e}")
            if i < attempts:
                time.sleep(sleep_seconds)
    raise last_err
