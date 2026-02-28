# python/fetch/calendar/03_extract_from_document.py
#
# Purpose:
# - Read python/Data/raw_data/calendar/calendar_document.html (captured network snapshot)
# - Extract embedded JS object: window.calendarComponentStates[1] = {...}
# - Convert JS object-literal to valid JSON text
# - Output normalized events to python/Data/raw_data/calendar/calendar_event.json (+ calendar_event.csv)
#
# Notes:
# - Console output is ASCII-only (Windows cp1252 safe).
# - This script expects the HTML snapshot file already exists (from 02_capture_document_html.py).

from __future__ import annotations

import json
import re
import traceback
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo


# -----------------------
# Config
# -----------------------
ART_DIR = Path("python") / "Data" / "raw_data" / "calendar"

IN_HTML = ART_DIR / "calendar_document.html"
OUT_EVENTS_JSON = ART_DIR / "calendar_all_event.json"
OUT_EVENTS_CSV = ART_DIR / "calendar_all_event.csv"
OUT_META = ART_DIR / "events.meta.json"
OUT_ERR = ART_DIR / "extract_error.txt"

MARKER = "window.calendarComponentStates[1] ="

BKK = ZoneInfo("Asia/Bangkok")

IMPACT_SCORE = {"high": 3, "medium": 2, "low": 1}


# -----------------------
# Helpers: safe parsing
# -----------------------
def ensure_dirs() -> None:
    ART_DIR.mkdir(parents=True, exist_ok=True)


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def strip_html_tags(s: str) -> str:
    return re.sub(r"<.*?>", "", s or "")


def extract_object_literal(html: str, marker: str) -> str:
    """
    Extract JS object literal starting at the first '{' after marker, using brace counting.
    Handles quotes + escapes so it won't break on braces inside strings.
    """
    i = html.find(marker)
    if i == -1:
        raise RuntimeError("Marker not found: " + marker)

    j = html.find("{", i)
    if j == -1:
        raise RuntimeError("Object start '{' not found after marker")

    in_str = False
    esc = False
    quote = ""
    depth = 0

    for k in range(j, len(html)):
        ch = html[k]

        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            continue

        if ch in ("'", '"'):
            in_str = True
            quote = ch
            continue

        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[j : k + 1]

    raise RuntimeError("Unbalanced braces while extracting object")


def quote_unquoted_keys(js: str) -> str:
    """
    Convert unquoted keys like: days: -> "days":
    Only when outside strings and right after { or , (ignoring whitespace).
    """
    out: list[str] = []
    i = 0
    in_str = False
    esc = False
    quote = ""

    while i < len(js):
        ch = js[i]

        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            i += 1
            continue

        if ch in ('"', "'"):
            in_str = True
            quote = ch
            out.append(ch)
            i += 1
            continue

        if ch.isalpha() or ch == "_":
            # previous non-space emitted char
            j = len(out) - 1
            while j >= 0 and out[j].isspace():
                j -= 1
            prev = out[j] if j >= 0 else ""

            if prev in ("{", ","):
                start = i
                k = i + 1
                while k < len(js) and (js[k].isalnum() or js[k] == "_"):
                    k += 1
                ident = js[start:k]

                m = k
                while m < len(js) and js[m].isspace():
                    m += 1

                if m < len(js) and js[m] == ":":
                    out.append(f'"{ident}":')
                    i = m + 1
                    continue

        out.append(ch)
        i += 1

    return "".join(out)


def single_quotes_to_double(js: str) -> str:
    """
    Convert single-quoted string literals to JSON double-quoted strings.
    Leaves double-quoted strings unchanged.
    """
    out: list[str] = []
    i = 0
    in_d = False
    in_s = False
    esc = False

    while i < len(js):
        ch = js[i]

        if in_d:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_d = False
            i += 1
            continue

        if in_s:
            if esc:
                out.append(ch)
                esc = False
                i += 1
                continue
            if ch == "\\":
                out.append(ch)
                esc = True
                i += 1
                continue
            if ch == "'":
                out.append('"')
                in_s = False
                i += 1
                continue
            if ch == '"':
                out.append('\\"')
            else:
                out.append(ch)
            i += 1
            continue

        if ch == '"':
            in_d = True
            out.append(ch)
            i += 1
            continue
        if ch == "'":
            in_s = True
            out.append('"')
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def strip_object_freeze(js: str) -> str:
    """
    Replace Object.freeze(<expr>) with <expr> outside strings.
    """
    out: list[str] = []
    i = 0
    in_str = False
    esc = False
    quote = ""

    while i < len(js):
        ch = js[i]

        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            i += 1
            continue

        if ch in ('"', "'"):
            in_str = True
            quote = ch
            out.append(ch)
            i += 1
            continue

        if js.startswith("Object.freeze(", i):
            i += len("Object.freeze(")
            par = 1
            while i < len(js) and par > 0:
                c = js[i]

                # preserve strings inside
                if c in ('"', "'"):
                    q = c
                    out.append(c)
                    i += 1
                    esc2 = False
                    while i < len(js):
                        cc = js[i]
                        out.append(cc)
                        if esc2:
                            esc2 = False
                        elif cc == "\\":
                            esc2 = True
                        elif cc == q:
                            i += 1
                            break
                        i += 1
                    continue

                if c == "(":
                    par += 1
                    out.append(c)
                elif c == ")":
                    par -= 1
                    if par == 0:
                        i += 1
                        break
                    out.append(c)
                else:
                    out.append(c)

                i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def remove_trailing_commas(js: str) -> str:
    """
    Remove trailing commas before } or ] outside strings.
    """
    out: list[str] = []
    i = 0
    in_str = False
    esc = False
    quote = ""

    while i < len(js):
        ch = js[i]

        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            i += 1
            continue

        if ch in ('"', "'"):
            in_str = True
            quote = ch
            out.append(ch)
            i += 1
            continue

        if ch == ",":
            j = i + 1
            while j < len(js) and js[j].isspace():
                j += 1
            if j < len(js) and js[j] in ("}", "]"):
                i += 1
                continue

        out.append(ch)
        i += 1

    return "".join(out)


def js_object_to_json_text(js_obj: str) -> str:
    s = quote_unquoted_keys(js_obj)
    s = single_quotes_to_double(s)
    s = strip_object_freeze(s)
    s = remove_trailing_commas(s)
    return s


def parse_epoch_to_bkk_iso(epoch: int | None) -> str:
    if not isinstance(epoch, int):
        return ""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(BKK).isoformat()


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    headers = list(rows[0].keys())

    def esc_csv(x):
        s = "" if x is None else str(x)
        if any(c in s for c in [",", '"', "\n"]):
            s = '"' + s.replace('"', '""') + '"'
        return s

    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(",".join(headers) + "\n")
        for r in rows:
            f.write(",".join(esc_csv(r.get(h)) for h in headers) + "\n")


def load_existing_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def merge_events(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged: dict[tuple[int, int], dict] = {}

    def key_of(row: dict) -> tuple[int, int] | None:
        event_id = row.get("event_id")
        epoch = row.get("dateline_epoch")
        if isinstance(event_id, int) and isinstance(epoch, int):
            return (event_id, epoch)
        return None

    for row in existing:
        key = key_of(row)
        if key:
            merged[key] = row

    for row in incoming:
        key = key_of(row)
        if not key:
            continue
        if key not in merged:
            merged[key] = row
            continue
        merged_row = dict(merged[key])
        new_actual = row.get("actual")
        if new_actual not in (None, ""):
            merged_row["actual"] = new_actual
        for field, value in row.items():
            if field == "actual":
                continue
            if value not in (None, ""):
                merged_row[field] = value
        merged[key] = merged_row

    return list(merged.values())


def sort_events_desc(rows: list[dict]) -> list[dict]:
    def sort_key(row: dict) -> tuple[int, int]:
        epoch = row.get("dateline_epoch")
        event_id = row.get("event_id")
        epoch_val = int(epoch) if isinstance(epoch, int) else -1
        event_val = int(event_id) if isinstance(event_id, int) else -1
        return (-epoch_val, -event_val)

    return sorted(rows, key=sort_key)


# -----------------------
# Main
# -----------------------
def main() -> None:
    ensure_dirs()

    if not IN_HTML.exists():
        raise FileNotFoundError("Missing input HTML: " + str(IN_HTML.resolve()))

    html = IN_HTML.read_text(encoding="utf-8", errors="ignore")

    js_obj = extract_object_literal(html, MARKER)
    json_text = js_object_to_json_text(js_obj)

    data = json.loads(json_text)

    # Normalize + dedupe
    rows: list[dict] = []
    seen: set[tuple[int, int]] = set()

    for day in data.get("days", []):
        day_label = strip_html_tags(day.get("date", ""))
        for ev in day.get("events", []):
            event_id = ev.get("id")
            epoch = ev.get("dateline")

            if not isinstance(event_id, int) or not isinstance(epoch, int):
                continue

            pk = (event_id, epoch)
            if pk in seen:
                continue
            seen.add(pk)

            impact = (ev.get("impactName") or "").lower().strip()
            impact_score = IMPACT_SCORE.get(impact, 0)

            rows.append(
                {
                    "day_label": day_label,
                    "event_id": event_id,
                    "dateline_epoch": epoch,
                    "datetime_bkk": parse_epoch_to_bkk_iso(epoch),
                    "currency": ev.get("currency"),
                    "country": ev.get("country"),
                    "impact": impact,
                    "impact_score": impact_score,
                    "timeLabel": ev.get("timeLabel"),
                    "name": ev.get("name"),
                    "prefixedName": ev.get("prefixedName"),
                    "actual": ev.get("actual"),
                    "forecast": ev.get("forecast"),
                    "previous": ev.get("previous"),
                    "revision": ev.get("revision"),
                    "url": ev.get("url"),
                    "soloUrl": ev.get("soloUrl"),
                }
            )

    existing_rows = load_existing_events(OUT_EVENTS_JSON)
    rows = merge_events(existing_rows, rows)
    rows = sort_events_desc(rows)

    OUT_EVENTS_JSON.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(rows, OUT_EVENTS_CSV)

    meta = {
        "generated_at_utc": iso_utc_now(),
        "input_html": str(IN_HTML.resolve()),
        "marker": MARKER,
        "events_count": len(rows),
        "output_events_json": str(OUT_EVENTS_JSON.resolve()),
        "output_events_csv": str(OUT_EVENTS_CSV.resolve()),
        "dedupe_key": "(event_id, dateline_epoch)",
        "merge_policy": "merge by key, overwrite actual when provided",
    }
    OUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print("OK events:", len(rows), flush=True)
    print("OK saved:", str(OUT_EVENTS_JSON.resolve()), flush=True)
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
