# python/transform/calendar/40_compute_surprise.py
#
# Purpose:
# - Read python/Data/raw_data/calendar/events.json (or events_merged.json)
# - Filter to currencies for a target pair (default EURUSD => EUR, USD)
# - Parse numeric strings (%, K/M/B, commas, etc.)
# - Compute surprise metrics:
#     - surprise = actual - forecast
#     - surprise_pct = (actual-forecast)/abs(forecast) * 100 (when possible)
# - Output python/Data/raw_data/calendar/event_surprises.json (+ meta)
#
# Notes:
# - ASCII-only console output (Windows cp1252 safe).
# - Keeps raw strings + parsed numbers for audit.

from __future__ import annotations

import argparse
import json
import math
import re
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"

DEFAULT_IN = ART_DIR / "events.json"
ALT_IN = ART_DIR / "events_merged.json"

OUT_SURPRISE = ART_DIR / "event_surprises.json"
OUT_META = ART_DIR / "event_surprises.meta.json"
OUT_ERR = ART_DIR / "surprise_error.txt"


IMPACT_SCORE = {"high": 3, "medium": 2, "low": 1}


def ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_pair_to_currencies(pair: str) -> set[str]:
    p = (pair or "").upper().strip()
    if len(p) == 6 and p.isalpha():
        return {p[:3], p[3:]}
    parts = [x.strip().upper() for x in p.replace("/", ",").split(",") if x.strip()]
    return set(parts) if parts else {"EUR", "USD"}


def load_events(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("events file must be a list")
    return data


# -----------------------
# Numeric parsing
# -----------------------
_SUFFIX = {
    "K": 1e3,
    "M": 1e6,
    "B": 1e9,
    "T": 1e12,
}


def _clean(s: str) -> str:
    return s.strip().replace("\u00a0", " ")  # NBSP


def parse_number(raw: Any) -> Optional[float]:
    """
    Parse common FF numeric formats:
    - "1.2", "-0.3"
    - "1.2%" => 1.2
    - "250K" => 250000
    - "1.2M" => 1200000
    - "1,234.5" => 1234.5
    - "N/A", "", None => None
    - Some fields might contain "—" or "n/a"
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and (math.isnan(raw) or math.isinf(raw)):
            return None
        return float(raw)

    s = _clean(str(raw))
    if s == "":
        return None

    s_low = s.lower()
    if s_low in {"n/a", "na", "none", "null", "--", "—", "-"}:
        return None

    # Remove surrounding parentheses for negatives: "(1.2)" => -1.2
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    # Percent sign: keep value numeric (1.2% => 1.2)
    if s.endswith("%"):
        s = s[:-1].strip()

    # Remove commas
    s = s.replace(",", "")

    # Handle suffix K/M/B/T at the end
    m = re.fullmatch(r"([+-]?\d+(?:\.\d+)?)([KMBT])", s, flags=re.I)
    if m:
        val = float(m.group(1))
        mul = _SUFFIX[m.group(2).upper()]
        val = val * mul
        return -val if neg else val

    # Basic float
    m2 = re.fullmatch(r"[+-]?\d+(?:\.\d+)?", s)
    if m2:
        val = float(s)
        return -val if neg else val

    # Sometimes FF includes "0.1 pips" or "3.2 pts" etc. Try to extract first number
    m3 = re.search(r"[+-]?\d+(?:\.\d+)?", s)
    if m3:
        val = float(m3.group(0))
        return -val if neg else val

    return None


def compute_surprise(actual: Optional[float], forecast: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if actual is None or forecast is None:
        return None, None
    s = actual - forecast
    if forecast == 0:
        return s, None
    sp = (s / abs(forecast)) * 100.0
    return s, sp


# -----------------------
# Output model
# -----------------------
@dataclass
class SurpriseRow:
    event_id: int
    dateline_epoch: int
    datetime_bkk: str
    currency: str
    impact: str
    impact_score: int
    name: str
    actual_raw: Any
    forecast_raw: Any
    previous_raw: Any
    actual: Optional[float]
    forecast: Optional[float]
    previous: Optional[float]
    surprise: Optional[float]
    surprise_pct: Optional[float]
    url: str | None
    soloUrl: str | None


def main() -> None:
    ensure_dirs()

    ap = argparse.ArgumentParser(description="Compute FF calendar surprise (actual vs forecast)")
    ap.add_argument("--pair", default="EURUSD", help="Target pair, default EURUSD (currencies EUR,USD)")
    ap.add_argument("--in", dest="in_path", default="", help="Input file (events.json or events_merged.json). Default: events.json if exists.")
    ap.add_argument("--min-impact", default="medium", choices=["low", "medium", "high"], help="Minimum impact to include")
    args = ap.parse_args()

    # pick input
    in_path = Path(args.in_path) if args.in_path else (DEFAULT_IN if DEFAULT_IN.exists() else ALT_IN)
    if not in_path.exists():
        raise FileNotFoundError("Missing input: " + str(in_path.resolve()))

    currencies = parse_pair_to_currencies(args.pair)

    min_score = IMPACT_SCORE.get(args.min_impact.lower(), 2)

    events = load_events(in_path)

    out: list[SurpriseRow] = []
    skipped_no_actual = 0
    skipped_no_forecast = 0

    for e in events:
        cur = (e.get("currency") or "").upper().strip()
        if cur not in currencies:
            continue

        impact = (e.get("impact") or "").lower().strip()
        score = int(e.get("impact_score") or IMPACT_SCORE.get(impact, 0))
        if score < min_score:
            continue

        actual_raw = e.get("actual")
        forecast_raw = e.get("forecast")
        previous_raw = e.get("previous")

        actual = parse_number(actual_raw)
        forecast = parse_number(forecast_raw)
        previous = parse_number(previous_raw)

        if actual is None:
            skipped_no_actual += 1
            continue
        if forecast is None:
            skipped_no_forecast += 1
            continue

        s, sp = compute_surprise(actual, forecast)

        try:
            event_id = int(e.get("event_id"))
            epoch = int(e.get("dateline_epoch"))
        except Exception:
            continue

        out.append(
            SurpriseRow(
                event_id=event_id,
                dateline_epoch=epoch,
                datetime_bkk=str(e.get("datetime_bkk") or ""),
                currency=cur,
                impact=impact,
                impact_score=score,
                name=str(e.get("name") or ""),
                actual_raw=actual_raw,
                forecast_raw=forecast_raw,
                previous_raw=previous_raw,
                actual=actual,
                forecast=forecast,
                previous=previous,
                surprise=s,
                surprise_pct=sp,
                url=e.get("url"),
                soloUrl=e.get("soloUrl"),
            )
        )

    out.sort(key=lambda r: (r.dateline_epoch, r.event_id))

    OUT_SURPRISE.write_text(
        json.dumps([asdict(r) for r in out], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    meta = {
        "generated_at_utc": iso_utc_now(),
        "input": str(in_path.resolve()),
        "pair": args.pair,
        "currencies": sorted(list(currencies)),
        "min_impact": args.min_impact,
        "min_score": min_score,
        "events_in": len(events),
        "surprises_out": len(out),
        "skipped_no_actual": skipped_no_actual,
        "skipped_no_forecast": skipped_no_forecast,
        "output": str(OUT_SURPRISE.resolve()),
    }
    OUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK surprises:", len(out), flush=True)
    print("OK saved    :", str(OUT_SURPRISE.resolve()), flush=True)
    print("OK meta     :", str(OUT_META.resolve()), flush=True)
    print("OK skipped_no_actual  :", skipped_no_actual, flush=True)
    print("OK skipped_no_forecast:", skipped_no_forecast, flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        ensure_dirs()
        OUT_ERR.write_text(traceback.format_exc(), encoding="utf-8")
        print("ERROR saved ->", str(OUT_ERR.resolve()), flush=True)
        input("Press Enter to exit...")
