# python/fetch/calendar/30_refresh_actuals.py
#
# Purpose:
# - Refresh ForexFactory calendar snapshot (02 + 03)
# - Merge newly available fields (especially "actual") into the previous events list
# - Keep a history snapshot (before/after) for audit/debug
#
# Inputs:
# - python/Data/raw_data/calendar/events.json (previous)
#
# Outputs:
# - python/Data/raw_data/calendar/events_merged.json
# - python/Data/raw_data/calendar/events_refresh.meta.json
# - python/Data/raw_data/calendar/history/<timestamp>/events_before.json
# - python/Data/raw_data/calendar/history/<timestamp>/events_after.json
#
# Notes:
# - Uses subprocess to run step scripts (works even if filenames start with digits).
# - ASCII-only console output for Windows cp1252 safety.

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any


# -----------------------
# Config paths
# -----------------------
ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"
IN_EVENTS = ART_DIR / "events.json"

OUT_MERGED = ART_DIR / "events_merged.json"
OUT_META = ART_DIR / "events_refresh.meta.json"
OUT_ERR = ART_DIR / "refresh_error.txt"

HISTORY_DIR = ART_DIR / "history"

# Step scripts (repo-relative)
STEP02 = Path("python") / "fetch" / "calendar" / "02_capture_document_html.py"
STEP03 = Path("python") / "fetch" / "calendar" / "03_extract_from_document.py"


@dataclass
class RefreshMeta:
    run_id: str
    ran_at_local: str
    step02_ok: bool
    step03_ok: bool
    before_count: int
    after_count: int
    merged_count: int
    matched: int
    added: int
    updated_any_field: int
    updated_actual: int
    newly_released_actual: int
    output_merged: str
    history_dir: str


def ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)


def now_run_id() -> str:
    # Windows-safe folder name
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")


def run_step(script_path: Path) -> None:
    if not script_path.exists():
        raise FileNotFoundError("Missing step script: " + str(script_path.resolve()))
    # Use current python executable, unbuffered
    cmd = [sys.executable, "-u", str(script_path)]
    proc = subprocess.run(cmd, cwd=str(Path.cwd()), capture_output=True, text=True)
    if proc.returncode != 0:
        msg = (
            "Step failed: " + str(script_path) + "\n"
            "STDOUT:\n" + proc.stdout + "\n"
            "STDERR:\n" + proc.stderr + "\n"
        )
        raise RuntimeError(msg)


def load_events(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("events file must be a list: " + str(path))
    return data


def pk(e: dict[str, Any]) -> tuple[int, int] | None:
    try:
        return (int(e["event_id"]), int(e["dateline_epoch"]))
    except Exception:
        return None


def is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def merge_events(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    Merge "after" into "before" by pk=(event_id, dateline_epoch).
    For matched events, update selected fields if changed.
    Append new events that were not in before.
    """
    before_map: dict[tuple[int, int], dict[str, Any]] = {}
    for e in before:
        k = pk(e)
        if k:
            before_map[k] = e

    matched = 0
    added = 0
    updated_any = 0
    updated_actual = 0
    newly_released = 0

    # Fields worth refreshing (safe + useful)
    REFRESH_FIELDS = [
        "actual",
        "forecast",
        "previous",
        "revision",
        "impact",
        "impact_score",
        "timeLabel",
        "prefixedName",
        "name",
        "url",
        "soloUrl",
    ]

    for a in after:
        k = pk(a)
        if not k:
            continue

        if k not in before_map:
            # New event
            before.append(a)
            before_map[k] = a
            added += 1
            continue

        # Matched: update selected fields
        b = before_map[k]
        matched += 1

        changed = False

        # track actual change semantics
        b_actual_before = b.get("actual")
        a_actual_after = a.get("actual")

        for f in REFRESH_FIELDS:
            if f in a:
                bv = b.get(f)
                av = a.get(f)
                if av != bv and not (is_blank(av) and is_blank(bv)):
                    b[f] = av
                    changed = True
                    if f == "actual":
                        updated_actual += 1

        # if actual was blank and now not blank -> "released"
        if (is_blank(b_actual_before) and not is_blank(a_actual_after)):
            newly_released += 1

        if changed:
            updated_any += 1

    # sort stable by time then event_id
    before.sort(key=lambda r: (int(r.get("dateline_epoch", 0)), int(r.get("event_id", 0))))

    stats = {
        "matched": matched,
        "added": added,
        "updated_any_field": updated_any,
        "updated_actual": updated_actual,
        "newly_released_actual": newly_released,
    }
    return before, stats


def main() -> None:
    ensure_dirs()

    ap = argparse.ArgumentParser(description="Refresh ForexFactory actuals by re-capturing and merging events.json")
    ap.add_argument("--keep-after", action="store_true", help="Keep the freshly extracted after-events as python/Data/raw_data/calendar/events_after.json")
    ap.add_argument("--overwrite-events", action="store_true", help="Overwrite python/Data/raw_data/calendar/events.json with merged output")
    args = ap.parse_args()

    if not IN_EVENTS.exists():
        raise FileNotFoundError("Missing input events.json: " + str(IN_EVENTS.resolve()))

    run_id = now_run_id()
    run_dir = HISTORY_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # 1) Load BEFORE
    before = load_events(IN_EVENTS)
    before_count = len(before)

    before_path = run_dir / "events_before.json"
    before_path.write_text(json.dumps(before, ensure_ascii=False, indent=2), encoding="utf-8")

    step02_ok = False
    step03_ok = False

    # 2) Run step02 + step03 to produce AFTER (this will rewrite python/Data/raw_data/calendar/events.json)
    #    So first copy current events.json to a safe place (we already wrote events_before.json).
    try:
        print("RUN step02 ...", flush=True)
        run_step(STEP02)
        step02_ok = True

        print("RUN step03 ...", flush=True)
        run_step(STEP03)
        step03_ok = True
    except Exception:
        # write error and stop
        raise

    # 3) Load AFTER (freshly extracted)
    after = load_events(IN_EVENTS)
    after_count = len(after)

    after_path = run_dir / "events_after.json"
    after_path.write_text(json.dumps(after, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.keep_after:
        (ART_DIR / "events_after.json").write_text(json.dumps(after, ensure_ascii=False, indent=2), encoding="utf-8")

    # 4) Merge and write merged output
    merged, stats = merge_events(before, after)
    merged_count = len(merged)

    OUT_MERGED.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.overwrite_events:
        IN_EVENTS.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    # 5) Meta
    meta = RefreshMeta(
        run_id=run_id,
        ran_at_local=datetime.now().isoformat(timespec="seconds"),
        step02_ok=step02_ok,
        step03_ok=step03_ok,
        before_count=before_count,
        after_count=after_count,
        merged_count=merged_count,
        matched=stats["matched"],
        added=stats["added"],
        updated_any_field=stats["updated_any_field"],
        updated_actual=stats["updated_actual"],
        newly_released_actual=stats["newly_released_actual"],
        output_merged=str(OUT_MERGED.resolve()),
        history_dir=str(run_dir.resolve()),
    )
    OUT_META.write_text(json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8")

    # ASCII-only summary
    print("OK before:", before_count, flush=True)
    print("OK after :", after_count, flush=True)
    print("OK merged:", merged_count, flush=True)
    print("OK matched:", stats["matched"], "added:", stats["added"], flush=True)
    print("OK updated_any:", stats["updated_any_field"], flush=True)
    print("OK updated_actual:", stats["updated_actual"], "newly_released:", stats["newly_released_actual"], flush=True)
    print("OK saved merged:", str(OUT_MERGED.resolve()), flush=True)
    print("OK saved meta  :", str(OUT_META.resolve()), flush=True)
    print("OK history dir :", str(run_dir.resolve()), flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        ensure_dirs()
        OUT_ERR.write_text(traceback.format_exc(), encoding="utf-8")
        print("ERROR saved ->", str(OUT_ERR.resolve()), flush=True)
        input("Press Enter to exit...")
