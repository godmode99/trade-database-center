# python/fetch/calendar/select_events.py
#
# Purpose:
# - Read python/Data/raw_data/calendar/calendar_all_event.json
# - Filter events using config.yaml options
# - Output python/Data/raw_data/calendar/calendar_select_events.json (+ csv, meta)
#
# Notes:
# - ASCII-only console output (Windows cp1252 safe).

from __future__ import annotations

import json
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from utils import load_config


# -----------------------
# Config
# -----------------------
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "app" / "config.yaml"

ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"

IN_EVENTS = ART_DIR / "calendar_all_event.json"
OUT_EVENTS_JSON = ART_DIR / "calendar_select_events.json"
OUT_LATEST_EVENTS_JSON = ART_DIR / "latest_select_events.json"
OUT_EVENTS_CSV = ART_DIR / "calendar_select_events.csv"
OUT_META = ART_DIR / "select_events.meta.json"
OUT_ERR = ART_DIR / "select_events_error.txt"


def ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    headers = list(rows[0].keys())

    def esc_csv(x: Any) -> str:
        s = "" if x is None else str(x)
        if any(c in s for c in [",", '"', "\n"]):
            s = '"' + s.replace('"', '""') + '"'
        return s

    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers) + "\n")
        for r in rows:
            f.write(",".join(esc_csv(r.get(h)) for h in headers) + "\n")


def load_events(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("calendar_all_event.json must be a list")
    return [row for row in data if isinstance(row, dict)]


def load_existing_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def event_key(event: dict) -> str:
    event_id = event.get("event_id")
    if event_id is not None and str(event_id).strip():
        return str(event_id).strip()
    dateline = event.get("dateline_epoch")
    currency = (event.get("currency") or "").strip()
    name = (event.get("name") or "").strip()
    return f"{dateline}|{currency}|{name}"


def has_actual(event: dict) -> bool:
    actual = event.get("actual")
    if actual is None:
        return False
    return str(actual).strip() != ""


def merge_events(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    order: list[str] = []

    for event in existing:
        key = event_key(event)
        if key not in merged:
            merged[key] = event
            order.append(key)

    for event in incoming:
        key = event_key(event)
        if key not in merged:
            merged[key] = event
            order.append(key)
            continue

        if has_actual(event) and not has_actual(merged[key]):
            merged[key] = event

    return [merged[key] for key in order]


def sort_events_desc(events: list[dict]) -> list[dict]:
    def sort_key(event: dict) -> tuple[int, int]:
        epoch = event.get("dateline_epoch")
        event_id = event.get("event_id")
        epoch_val = int(epoch) if isinstance(epoch, int) else -1
        event_val = int(event_id) if isinstance(event_id, int) else -1
        return (-epoch_val, -event_val)

    return sorted(events, key=sort_key)


def normalize_list(values: Any) -> list[str]:
    if not values:
        return []
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()]
    return [str(values).strip()] if str(values).strip() else []


def filter_events(events: list[dict], cfg: dict) -> list[dict]:
    filters = cfg.get("select_events", {}) or {}

    currencies = {c.upper() for c in normalize_list(filters.get("currencies"))}
    impacts = {i.lower() for i in normalize_list(filters.get("impacts"))}
    countries = {c.lower() for c in normalize_list(filters.get("countries"))}
    name_keywords = [k.lower() for k in normalize_list(filters.get("name_keywords"))]
    exclude_keywords = [k.lower() for k in normalize_list(filters.get("exclude_name_keywords"))]

    impact_score_min = filters.get("impact_score_min")
    if impact_score_min is not None:
        try:
            impact_score_min = int(impact_score_min)
        except Exception:
            impact_score_min = None

    days_back = filters.get("days_back")
    if days_back is not None:
        try:
            days_back = int(days_back)
        except Exception:
            days_back = None

    days_forward = filters.get("days_forward")
    if days_forward is not None:
        try:
            days_forward = int(days_forward)
        except Exception:
            days_forward = None

    if days_back is None:
        days_back = -5
    if days_forward is None:
        days_forward = 5

    start_epoch: float | None = None
    end_epoch: float | None = None
    if days_back is not None or days_forward is not None:
        now = datetime.now(timezone.utc)
        if days_back is not None:
            start_epoch = (now + timedelta(days=days_back)).timestamp()
        if days_forward is not None:
            end_epoch = (now + timedelta(days=days_forward)).timestamp()

    out: list[dict] = []

    for e in events:
        currency = (e.get("currency") or "").upper().strip()
        impact = (e.get("impact") or "").lower().strip()
        country = (e.get("country") or "").lower().strip()
        name = (e.get("name") or "").lower().strip()

        if currencies and currency not in currencies:
            continue
        if impacts and impact not in impacts:
            continue
        if countries and country not in countries:
            continue
        if impact_score_min is not None:
            score = e.get("impact_score")
            try:
                score_val = int(score)
            except Exception:
                score_val = 0
            if score_val < impact_score_min:
                continue
        if start_epoch is not None or end_epoch is not None:
            epoch_val = e.get("dateline_epoch")
            try:
                epoch_val = int(epoch_val)
            except Exception:
                epoch_val = None
            if epoch_val is None:
                continue
            if start_epoch is not None and epoch_val < start_epoch:
                continue
            if end_epoch is not None and epoch_val > end_epoch:
                continue
        if name_keywords and not any(k in name for k in name_keywords):
            continue
        if exclude_keywords and any(k in name for k in exclude_keywords):
            continue

        out.append(e)

    return out


def main() -> None:
    ensure_dirs()

    if not IN_EVENTS.exists():
        raise FileNotFoundError("Missing input: " + str(IN_EVENTS.resolve()))

    cfg = load_config(str(CONFIG_PATH)) if CONFIG_PATH.exists() else {}

    events = load_events(IN_EVENTS)
    selected = filter_events(events, cfg)
    existing_selected = load_existing_events(OUT_EVENTS_JSON)
    merged_selected = merge_events(existing_selected, selected)
    existing_keys = {event_key(ev) for ev in existing_selected}
    latest_selected = [ev for ev in selected if event_key(ev) not in existing_keys]
    merged_selected = sort_events_desc(merged_selected)
    latest_selected = sort_events_desc(latest_selected)

    if selected:
        print("รายละเอียด select_events", flush=True)
        print("เวลาข่าวออก | currency | impact | name | actual", flush=True)
        for ev in selected:
            time_label = ev.get("datetime_bkk") or ev.get("timeLabel") or ""
            currency = ev.get("currency") or ""
            impact = ev.get("impact") or ""
            name = ev.get("name") or ""
            actual = ev.get("actual") or ""
            print(
                f"{time_label} | {currency} | {impact} | {name} | {actual}",
                flush=True,
            )

    OUT_LATEST_EVENTS_JSON.write_text(
        json.dumps(latest_selected, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    OUT_EVENTS_JSON.write_text(
        json.dumps(merged_selected, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_csv(merged_selected, OUT_EVENTS_CSV)

    meta = {
        "generated_at_utc": iso_utc_now(),
        "input_events_json": str(IN_EVENTS.resolve()),
        "output_events_json": str(OUT_EVENTS_JSON.resolve()),
        "output_latest_events_json": str(OUT_LATEST_EVENTS_JSON.resolve()),
        "output_events_csv": str(OUT_EVENTS_CSV.resolve()),
        "selected_count": len(merged_selected),
        "latest_selected_count": len(latest_selected),
        "filters": cfg.get("select_events", {}) or {},
    }
    OUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK selected:", len(selected), flush=True)
    print("OK saved:", str(OUT_EVENTS_JSON.resolve()), flush=True)
    print("OK saved:", str(OUT_LATEST_EVENTS_JSON.resolve()), flush=True)
    print("OK saved:", str(OUT_EVENTS_CSV.resolve()), flush=True)
    print("OK saved:", str(OUT_META.resolve()), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        ensure_dirs()
        OUT_ERR.write_text(traceback.format_exc(), encoding="utf-8")
        print("ERROR saved ->", str(OUT_ERR.resolve()), flush=True)
        input("Press Enter to exit...")
