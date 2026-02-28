#!/usr/bin/env python3
"""Calendar fetch pipeline + Supabase upsert.

Usage:
  python main.py --config config.yaml
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error, parse, request


def load_config(path: str) -> dict[str, Any]:
    text = open(path, "r", encoding="utf-8").read()
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
    except Exception:
        data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("config root must be a mapping")
    return data


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_datetime_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    raw = str(value).strip()
    if not raw:
        return None

    if raw.isdigit():
        return parse_datetime_to_iso(int(raw))

    # normalize Z timezone for fromisoformat
    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except ValueError:
        pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%m-%d-%Y %H:%M:%S",
        "%m-%d-%Y %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    return None


def impact_score(impact: str | None) -> int | None:
    if not impact:
        return None
    v = impact.strip().lower()
    if "high" in v:
        return 3
    if "medium" in v or "med" in v:
        return 2
    if "low" in v:
        return 1
    return None


def normalize_event(item: dict[str, Any], source_name: str) -> dict[str, Any] | None:
    event_id = item.get("event_id") or item.get("id") or item.get("eventId")
    dateline_epoch = item.get("dateline_epoch") or item.get("timestamp") or item.get("date")

    event_time_utc = (
        parse_datetime_to_iso(item.get("event_time_utc"))
        or parse_datetime_to_iso(item.get("event_time"))
        or parse_datetime_to_iso(dateline_epoch)
    )
    if not event_time_utc:
        return None

    if isinstance(dateline_epoch, str) and dateline_epoch.isdigit():
        dateline_epoch = int(dateline_epoch)
    elif isinstance(dateline_epoch, (int, float)):
        dateline_epoch = int(dateline_epoch)
    else:
        dateline_epoch = None

    event_name = item.get("event_name") or item.get("name") or item.get("title")
    if not event_name:
        return None

    if not event_id:
        event_id = f"{source_name}:{event_name}:{event_time_utc}"

    impact = item.get("impact") or item.get("volatility")

    payload = dict(item)
    return {
        "source": "calendar",
        "source_ref": source_name,
        "event_id": str(event_id),
        "event_time_utc": event_time_utc,
        "event_time_bkk": parse_datetime_to_iso(item.get("event_time_bkk")),
        "dateline_epoch": dateline_epoch,
        "currency": item.get("currency") or item.get("ccy"),
        "country": item.get("country"),
        "impact": impact,
        "impact_score": impact_score(impact),
        "event_name": str(event_name),
        "event_name_prefixed": item.get("event_name_prefixed") or item.get("prefixedName"),
        "actual": item.get("actual"),
        "forecast": item.get("forecast"),
        "previous": item.get("previous"),
        "revision": item.get("revision"),
        "url": item.get("url"),
        "solo_url": item.get("solo_url") or item.get("soloUrl"),
        "payload": payload,
    }


def http_json(method: str, url: str, headers: dict[str, str], body: Any | None = None) -> Any:
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers = {**headers, "Content-Type": "application/json"}

    req = request.Request(url, method=method.upper(), headers=headers, data=data)
    try:
        with request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else None
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} {url}: {detail}") from exc


@dataclass
class SupabaseClient:
    base_url: str
    api_key: str

    def _headers(self, schema: str, prefer: str | None = None) -> dict[str, str]:
        headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Profile": schema,
            "Accept-Profile": schema,
        }
        if prefer:
            headers["Prefer"] = prefer
        return headers

    def insert_pipeline_run(self, row: dict[str, Any]) -> None:
        http_json(
            "POST",
            f"{self.base_url}/rest/v1/pipeline_runs",
            self._headers("ops", "return=minimal"),
            [row],
        )

    def update_pipeline_run(self, run_id: str, row: dict[str, Any]) -> None:
        q = parse.urlencode({"run_id": f"eq.{run_id}"})
        http_json(
            "PATCH",
            f"{self.base_url}/rest/v1/pipeline_runs?{q}",
            self._headers("ops", "return=minimal"),
            row,
        )

    def upsert_calendar_events(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        q = parse.urlencode({"on_conflict": "source,event_id,event_time_utc"})
        http_json(
            "POST",
            f"{self.base_url}/rest/v1/calendar_events?{q}",
            self._headers("raw", "resolution=merge-duplicates,return=representation"),
            rows,
        )
        return len(rows)


def fetch_source(source_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    url = source_cfg["url"]
    method = source_cfg.get("method", "GET")
    headers = source_cfg.get("headers", {})
    payload = source_cfg.get("payload")
    data = http_json(method, url, headers, payload)
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        events_key = source_cfg.get("events_key", "events")
        maybe = data.get(events_key)
        if isinstance(maybe, list):
            return [x for x in maybe if isinstance(x, dict)]
    raise ValueError("source response must be list[object] or object with events key")


def run(config_path: str, dry_run: bool) -> None:
    cfg_path = Path(config_path)
    if not cfg_path.is_absolute() and not cfg_path.exists():
        cfg_path = Path(__file__).resolve().parent / cfg_path

    cfg = load_config(str(cfg_path))
    source_cfg = cfg["source"]
    db_cfg = cfg["database"]
    batch_size = int(cfg.get("pipeline", {}).get("batch_size", 500))

    source_name = source_cfg.get("name", "calendar_api")
    run_id = f"calendar_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"

    raw_items = fetch_source(source_cfg)
    normalized: list[dict[str, Any]] = []
    for item in raw_items:
        row = normalize_event(item, source_name)
        if row:
            row["run_id"] = run_id
            normalized.append(row)

    print(f"Fetched items={len(raw_items)} normalized={len(normalized)}")
    if dry_run:
        print("Dry run mode: skip database write")
        return

    base_url = os.getenv(db_cfg.get("url_env", "SUPABASE_URL"), db_cfg.get("url"))
    api_key = os.getenv(db_cfg.get("service_key_env", "SUPABASE_SERVICE_ROLE_KEY"), db_cfg.get("service_role_key"))
    if not base_url or not api_key:
        raise ValueError("Missing Supabase URL/API key in env or config")

    client = SupabaseClient(base_url=base_url.rstrip("/"), api_key=api_key)

    client.insert_pipeline_run(
        {
            "run_id": run_id,
            "pipeline_name": "calendar_fetch",
            "run_mode": cfg.get("pipeline", {}).get("run_mode", "manual"),
            "status": "running",
            "trigger_ref": cfg.get("pipeline", {}).get("trigger_ref"),
            "source_ref": source_name,
            "rows_read": len(raw_items),
            "metadata": {"config": os.path.basename(config_path), "started_at": now_utc_iso()},
        }
    )

    written = 0
    try:
        for i in range(0, len(normalized), batch_size):
            chunk = normalized[i : i + batch_size]
            written += client.upsert_calendar_events(chunk)

        client.update_pipeline_run(
            run_id,
            {
                "status": "success",
                "ended_at_utc": now_utc_iso(),
                "rows_written": written,
                "rows_read": len(raw_items),
            },
        )
        print(f"Database upsert done: {written} rows")
    except Exception as exc:
        client.update_pipeline_run(
            run_id,
            {
                "status": "failed",
                "ended_at_utc": now_utc_iso(),
                "rows_written": written,
                "rows_read": len(raw_items),
                "error_message": str(exc),
            },
        )
        raise


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch economic calendar and upsert to Supabase")
    ap.add_argument("--config", default="config.yaml", help="Path to YAML/JSON config")
    ap.add_argument("--dry-run", action="store_true", help="Fetch + normalize only")
    args = ap.parse_args()
    run(args.config, args.dry_run)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
