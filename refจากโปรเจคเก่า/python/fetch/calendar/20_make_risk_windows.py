# python/transform/calendar/20_make_risk_windows.py
#
# Purpose:
# - Read python/Data/raw_data/calendar/events.json
# - Filter for a target pair (default: EURUSD => currencies EUR, USD)
# - Build risk windows (no-trade windows) around events based on impact
# - Output python/Data/raw_data/calendar/no_trade_windows.json (+ meta)
#
# Notes:
# - ASCII-only console output (Windows cp1252 safe).
# - This file does NOT fetch the web. It only transforms events.json.

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable


# -----------------------
# Config
# -----------------------
ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"

IN_EVENTS = ART_DIR / "events.json"
OUT_WINDOWS = ART_DIR / "no_trade_windows.json"
OUT_META = ART_DIR / "no_trade_windows.meta.json"
OUT_ERR = ART_DIR / "risk_windows_error.txt"

DEFAULT_PAIR = "EURUSD"

# Default risk window rules (minutes)
# You can tune later:
DEFAULT_RULES_MINUTES = {
    "high":   {"pre": 60, "post": 30},
    "medium": {"pre": 30, "post": 15},
    "low":    {"pre": 0,  "post": 0},   # ignore low by default
}

# Use fixed +07:00 to avoid Windows TZ issues; your event epoch is UTC remember.
BKK_TZ = timezone(timedelta(hours=7))


# -----------------------
# Data model
# -----------------------
@dataclass(frozen=True)
class Window:
    event_id: int
    currency: str
    impact: str
    name: str
    dateline_epoch: int
    start_iso_bkk: str
    end_iso_bkk: str
    start_epoch: int
    end_epoch: int
    source: str  # "forexfactory"
    soloUrl: str | None = None


def ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_pair_to_currencies(pair: str) -> set[str]:
    p = (pair or "").upper().strip()
    if len(p) == 6 and p.isalpha():
        return {p[:3], p[3:]}
    # fallback: treat as comma-separated currencies
    parts = [x.strip().upper() for x in p.replace("/", ",").split(",") if x.strip()]
    return set(parts) if parts else {"EUR", "USD"}


def to_dt_bkk(epoch: int) -> datetime:
    return datetime.fromtimestamp(int(epoch), tz=timezone.utc).astimezone(BKK_TZ)


def load_events(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("events.json must be a list")
    return data


def build_windows(events: Iterable[dict], rules: dict) -> list[Window]:
    windows: list[Window] = []

    for e in events:
        try:
            event_id = int(e.get("event_id"))
            epoch = int(e.get("dateline_epoch"))
        except Exception:
            continue

        currency = (e.get("currency") or "").upper().strip()
        impact = (e.get("impact") or "").lower().strip()
        name = (e.get("name") or "").strip()

        if impact not in rules:
            continue

        pre = int(rules[impact]["pre"])
        post = int(rules[impact]["post"])
        if pre == 0 and post == 0:
            continue  # ignore low by default

        t = to_dt_bkk(epoch)
        start_dt = t - timedelta(minutes=pre)
        end_dt = t + timedelta(minutes=post)

        w = Window(
            event_id=event_id,
            currency=currency,
            impact=impact,
            name=name,
            dateline_epoch=epoch,
            start_iso_bkk=start_dt.isoformat(),
            end_iso_bkk=end_dt.isoformat(),
            start_epoch=int(start_dt.astimezone(timezone.utc).timestamp()),
            end_epoch=int(end_dt.astimezone(timezone.utc).timestamp()),
            source="forexfactory",
            soloUrl=e.get("soloUrl"),
        )
        windows.append(w)

    # Sort and merge overlaps per currency group (optional later)
    windows.sort(key=lambda x: (x.start_epoch, x.end_epoch, x.currency))
    return windows


def merge_overlaps(windows: list[Window]) -> list[Window]:
    """
    Optional: Merge overlapping windows across same currency+impact bucket.
    For trading, many prefer merge by *all* currencies together.
    Here we merge by currency only (simple).
    """
    if not windows:
        return []

    merged: list[Window] = []
    cur = windows[0]

    for w in windows[1:]:
        if w.currency == cur.currency and w.start_epoch <= cur.end_epoch:
            # merge time range
            new_start_epoch = min(cur.start_epoch, w.start_epoch)
            new_end_epoch = max(cur.end_epoch, w.end_epoch)
            new_start_dt = datetime.fromtimestamp(new_start_epoch, tz=timezone.utc).astimezone(BKK_TZ)
            new_end_dt = datetime.fromtimestamp(new_end_epoch, tz=timezone.utc).astimezone(BKK_TZ)

            cur = Window(
                event_id=cur.event_id,  # keep first
                currency=cur.currency,
                impact=cur.impact,      # keep first
                name=f"{cur.name} | {w.name}",
                dateline_epoch=cur.dateline_epoch,
                start_iso_bkk=new_start_dt.isoformat(),
                end_iso_bkk=new_end_dt.isoformat(),
                start_epoch=new_start_epoch,
                end_epoch=new_end_epoch,
                source=cur.source,
                soloUrl=cur.soloUrl,
            )
        else:
            merged.append(cur)
            cur = w

    merged.append(cur)
    return merged


def main(pair: str = DEFAULT_PAIR, do_merge: bool = True) -> None:
    ensure_dirs()

    if not IN_EVENTS.exists():
        raise FileNotFoundError("Missing input: " + str(IN_EVENTS.resolve()))

    currencies = parse_pair_to_currencies(pair)

    events = load_events(IN_EVENTS)
    filtered = [e for e in events if (e.get("currency") or "").upper().strip() in currencies]

    windows = build_windows(filtered, DEFAULT_RULES_MINUTES)
    windows_out = merge_overlaps(windows) if do_merge else windows

    OUT_WINDOWS.write_text(
        json.dumps([asdict(w) for w in windows_out], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    meta = {
        "generated_at_utc": iso_utc_now(),
        "input_events": str(IN_EVENTS.resolve()),
        "pair": pair,
        "currencies": sorted(list(currencies)),
        "rules_minutes": DEFAULT_RULES_MINUTES,
        "events_in": len(events),
        "events_filtered": len(filtered),
        "windows_raw": len(windows),
        "windows_output": len(windows_out),
        "output_windows": str(OUT_WINDOWS.resolve()),
        "merged_overlaps": do_merge,
    }
    OUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK events filtered:", len(filtered), flush=True)
    print("OK windows out    :", len(windows_out), flush=True)
    print("OK saved windows  :", str(OUT_WINDOWS.resolve()), flush=True)
    print("OK saved meta     :", str(OUT_META.resolve()), flush=True)


if __name__ == "__main__":
    try:
        # Change these if you want:
        # - pair="EURUSD", do_merge=True
        main(pair=DEFAULT_PAIR, do_merge=True)
    except Exception:
        ensure_dirs()
        OUT_ERR.write_text(traceback.format_exc(), encoding="utf-8")
        print("ERROR saved ->", str(OUT_ERR.resolve()), flush=True)
        input("Press Enter to exit...")
