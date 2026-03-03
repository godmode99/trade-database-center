from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ART_DIR = Path("artifacts") / "fedwatch"
LATEST_DIR = ART_DIR / "latest"
HISTORY_DIR = ART_DIR / "history"


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _find_previous_normalized(current_run_dir: Path) -> Path | None:
    if not HISTORY_DIR.exists():
        return None

    candidates = sorted([p for p in HISTORY_DIR.glob("*/normalized.json")])
    if not candidates:
        return None

    current_norm = current_run_dir / "normalized.json"
    for path in reversed(candidates):
        if path.resolve() != current_norm.resolve():
            return path
    return None


def _index_by_meeting(meetings: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {}
    for meeting in meetings:
        key = meeting.get("meeting_date")
        if key:
            out[key] = meeting
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", default="")
    parser.add_argument("--previous", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    current_path = Path(args.current) if args.current else (LATEST_DIR / "normalized.json")
    if not current_path.exists():
        raise FileNotFoundError(f"Missing current normalized.json: {current_path.resolve()}")

    current = json.loads(current_path.read_text(encoding="utf-8"))
    current_meetings = current.get("meetings", [])

    previous_path = Path(args.previous) if args.previous else _find_previous_normalized(current_path.parent)
    previous = {}
    if previous_path and previous_path.exists():
        previous = json.loads(previous_path.read_text(encoding="utf-8"))
    previous_meetings = previous.get("meetings", [])

    current_index = _index_by_meeting(current_meetings)
    previous_index = _index_by_meeting(previous_meetings)

    deltas: list[dict[str, Any]] = []

    for meeting_date, cur in current_index.items():
        prev = previous_index.get(meeting_date, {})
        delta_entry = {
            "meeting_date": meeting_date,
            "expected_rate_mid_change": None,
            "top_scenario_change": None,
            "prob_shift": [],
        }

        if prev:
            prev_expected = prev.get("expected_rate_mid")
            cur_expected = cur.get("expected_rate_mid")
            if isinstance(prev_expected, (int, float)) and isinstance(cur_expected, (int, float)):
                delta_entry["expected_rate_mid_change"] = cur_expected - prev_expected

            prev_top = prev.get("top_scenario", {}).get("rate_range") if prev.get("top_scenario") else None
            cur_top = cur.get("top_scenario", {}).get("rate_range") if cur.get("top_scenario") else None
            if prev_top or cur_top:
                delta_entry["top_scenario_change"] = {"from": prev_top, "to": cur_top}

            prev_dist = {d.get("rate_range"): float(d.get("prob", 0.0)) for d in prev.get("distribution", [])}
            cur_dist = {d.get("rate_range"): float(d.get("prob", 0.0)) for d in cur.get("distribution", [])}
            all_ranges = sorted(set(prev_dist) | set(cur_dist))
            for rate_range in all_ranges:
                delta_entry["prob_shift"].append(
                    {
                        "rate_range": rate_range,
                        "delta": cur_dist.get(rate_range, 0.0) - prev_dist.get(rate_range, 0.0),
                    }
                )

        deltas.append(delta_entry)

    out = {
        "asof_utc": _iso_utc_now(),
        "current_asof_utc": current.get("asof_utc"),
        "previous_asof_utc": previous.get("asof_utc") if previous else None,
        "deltas": deltas,
    }

    output_path = Path(args.output) if args.output else (current_path.parent / "delta.json")
    output_path.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
