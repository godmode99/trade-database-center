from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ART_DIR = Path("artifacts") / "fedwatch"
LATEST_DIR = ART_DIR / "latest"


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _format_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _find_next_meeting(meetings: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not meetings:
        return None
    sorted_meetings = sorted(
        meetings,
        key=lambda meeting: meeting.get("meeting_date") or "",
    )
    return sorted_meetings[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--normalized", default="")
    parser.add_argument("--delta", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    normalized_path = Path(args.normalized) if args.normalized else (LATEST_DIR / "normalized.json")
    delta_path = Path(args.delta) if args.delta else (LATEST_DIR / "delta.json")

    if not normalized_path.exists():
        raise FileNotFoundError(f"Missing normalized.json: {normalized_path.resolve()}")

    normalized = json.loads(normalized_path.read_text(encoding="utf-8"))
    delta = {}
    if delta_path.exists():
        delta = json.loads(delta_path.read_text(encoding="utf-8"))

    meetings = normalized.get("meetings", [])
    next_meeting = _find_next_meeting(meetings)

    digest_text = "FedWatch data unavailable."
    if next_meeting:
        top = next_meeting.get("top_scenario") or {}
        top_range = top.get("rate_range")
        top_prob = top.get("prob")

        delta_entry = None
        for item in delta.get("deltas", []):
            if item.get("meeting_date") == next_meeting.get("meeting_date"):
                delta_entry = item
                break

        change_text = ""
        if delta_entry:
            expected_change = delta_entry.get("expected_rate_mid_change")
            if isinstance(expected_change, (int, float)):
                change_text = f" Expected rate {expected_change:+.2f}."

        digest_text = (
            f"Next meeting {next_meeting.get('meeting_date')}: "
            f"Top scenario {top_range} @ {_format_percent(top_prob)}.{change_text}"
        )

    output = {
        "generated_at_utc": _iso_utc_now(),
        "summary": digest_text,
        "source": "fedwatch",
    }

    output_path = Path(args.output) if args.output else (normalized_path.parent / "digest.json")
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
