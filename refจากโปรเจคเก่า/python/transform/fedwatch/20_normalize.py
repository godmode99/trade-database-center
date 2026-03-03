from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ART_DIR = Path("artifacts") / "fedwatch"
LATEST_DIR = ART_DIR / "latest"
RUNS_DIR = ART_DIR / "runs"


@dataclass
class ValidationResult:
    ok: bool
    issues: list[str]


def _iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _parse_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir:
        return Path(args.run_dir)
    return LATEST_DIR


def _rate_mid(rate_range: str) -> float | None:
    match = re.findall(r"-?\d+(?:\.\d+)?", rate_range)
    if len(match) >= 2:
        low = float(match[0])
        high = float(match[1])
        return (low + high) / 2
    return None


def _sum_prob(dist: list[dict[str, Any]]) -> float:
    return sum(float(item.get("prob", 0.0)) for item in dist)


def _compute_prob_groups(distribution: list[dict[str, Any]], current_range: str | None) -> dict[str, float | None]:
    if not current_range:
        return {"prob_cut": None, "prob_hold": None, "prob_hike": None}
    current_mid = _rate_mid(current_range)
    if current_mid is None:
        return {"prob_cut": None, "prob_hold": None, "prob_hike": None}

    prob_cut = 0.0
    prob_hold = 0.0
    prob_hike = 0.0

    for item in distribution:
        mid = _rate_mid(item.get("rate_range", ""))
        if mid is None:
            continue
        prob = float(item.get("prob", 0.0))
        if mid < current_mid:
            prob_cut += prob
        elif mid > current_mid:
            prob_hike += prob
        else:
            prob_hold += prob

    return {"prob_cut": prob_cut, "prob_hold": prob_hold, "prob_hike": prob_hike}


def _validate(meetings: list[dict[str, Any]]) -> ValidationResult:
    issues: list[str] = []
    if not meetings:
        issues.append("no meetings extracted")
    for meeting in meetings:
        meeting_date = meeting.get("meeting_date")
        if not meeting_date or not isinstance(meeting_date, str):
            issues.append("meeting_date missing")
        elif not re.match(r"\d{4}-\d{2}-\d{2}$", meeting_date):
            issues.append(f"meeting_date not ISO: {meeting_date}")

        dist = meeting.get("distribution", [])
        total = _sum_prob(dist)
        if not (0.98 <= total <= 1.02):
            issues.append(f"prob sum out of range for {meeting_date}: {total:.4f}")

    return ValidationResult(ok=not issues, issues=issues)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", default="")
    args = parser.parse_args()

    run_dir = _parse_run_dir(args)
    run_dir.mkdir(parents=True, exist_ok=True)

    in_raw = run_dir / "raw.json"
    if not in_raw.exists():
        raise FileNotFoundError(f"Missing input raw.json: {in_raw.resolve()}")

    raw = json.loads(in_raw.read_text(encoding="utf-8"))
    meetings_raw = raw.get("meetings", [])
    current_range = raw.get("current_target_range")

    meetings: list[dict[str, Any]] = []
    for item in meetings_raw:
        dist = item.get("distribution", [])
        expected_rate_mid = 0.0
        for row in dist:
            mid = _rate_mid(row.get("rate_range", ""))
            prob = float(row.get("prob", 0.0))
            if mid is None:
                continue
            expected_rate_mid += mid * prob

        top = None
        if dist:
            top = max(dist, key=lambda d: float(d.get("prob", 0.0)))

        prob_groups = _compute_prob_groups(dist, current_range)

        meetings.append(
            {
                "meeting_date": item.get("meeting_date"),
                "distribution": dist,
                "expected_rate_mid": expected_rate_mid if dist else None,
                "top_scenario": top,
                **prob_groups,
            }
        )

    normalized = {
        "asof_utc": raw.get("asof_utc") or _iso_utc_now(),
        "source": "fedwatch",
        "asof_text": raw.get("asof_text"),
        "current_target_range": current_range,
        "meetings": meetings,
        "generated_at_utc": _iso_utc_now(),
    }

    validation = _validate(meetings)
    normalized["validation"] = {"ok": validation.ok, "issues": validation.issues}

    out_norm = run_dir / "normalized.json"
    out_norm.write_text(json.dumps(normalized, indent=2, ensure_ascii=False), encoding="utf-8")

    if not validation.ok:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
