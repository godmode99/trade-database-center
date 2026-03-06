from __future__ import annotations
import os, json, time, logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
import pandas as pd
from zoneinfo import ZoneInfo


TH_TZ = ZoneInfo("Asia/Bangkok")


def date_th_compact() -> str:
    # YYYYMMDD (TH)
    return datetime.now(TH_TZ).strftime("%Y%m%d")


def timestamp_th_compact() -> str:
    # DDMMYY_HHMM (TH)
    return datetime.now(TH_TZ).strftime("%d%m%y_%H%M")


def timestamp_th_compact_with_t() -> str:
    # DDMMYYTHHMM (TH)
    return datetime.now(TH_TZ).strftime("%d%m%yT%H%M")

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


def th_now_iso() -> str:
    return datetime.now(TH_TZ).replace(microsecond=0).isoformat()


def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    atomic_write_text(path, text)


def build_output_filename(
    symbol: str,
    label: str,
    output_format: str,
    timestamp: str,
    timeframe_label: str | None = None,
) -> str:
    ext = output_format.lower()
    prefix = f"{timeframe_label}_" if timeframe_label else ""
    return f"{prefix}raw_{symbol.lower()}_{label}_{timestamp}.{ext}"


def build_feature_filename(
    symbol: str,
    label: str,
    output_format: str,
    timestamp: str,
    timeframe_label: str | None = None,
) -> str:
    ext = output_format.lower()
    prefix = f"{timeframe_label}_" if timeframe_label else ""
    return f"{prefix}feature_{symbol.lower()}_{label}_{timestamp}.{ext}"


def save_json(df, path: Path) -> None:
    out = df.copy()
    out["time_th"] = out["time_th"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    payload = out.to_dict(orient="records")
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def load_cache_json(path: Path):
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    df = pd.DataFrame(payload)
    if "time_th" in df.columns:
        df["time_th"] = pd.to_datetime(df["time_th"])
    elif "time_utc" in df.columns:
        df["time_th"] = pd.to_datetime(df["time_utc"], utc=True).dt.tz_convert(TH_TZ)
        df = df.drop(columns=["time_utc"])
    return df


def find_latest_cache(
    data_dir: Path,
    symbol: str,
    label: str,
    ext: str,
    timeframe_label: str | None = None,
) -> Path | None:
    prefix = f"{timeframe_label}_" if timeframe_label else ""
    pattern = f"{prefix}raw_{symbol.lower()}_{label}_*.{ext}"
    candidates = list(data_dir.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def setup_logger(logs_dir: Path, name: str = "fetch") -> logging.Logger:
    ensure_dir(logs_dir)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = logs_dir / f"{name}_{datetime.now(TH_TZ).strftime('%Y%m%d')}.log"
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
