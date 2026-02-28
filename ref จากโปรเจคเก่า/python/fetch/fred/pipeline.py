from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from utils import (
    ensure_dir,
    utc_now_iso,
    thai_now_iso,
    atomic_write_json,
    date_th_compact,
    datetime_th_compact,
    retry,
)
from fred_client import fetch_fred_series_observations


@dataclass
class SourceStatus:
    ok: bool
    rows: int
    latest: str | None
    used_cache: bool
    error: str | None


def run_fetch_pipeline(cfg: Dict[str, Any], logger, base_dir: Path) -> Dict[str, Any]:
    """
    Policy:
      - data JSON snapshot: Data/raw_data/fred/<mode>/<YYYYMMDD_HHMMSS>.json
      - latest manifest overwrite: fetch_manifest.json
      - archive manifest dated: fetch_manifest_YYYYMMDD.json
      - error report dated on failures: fetch_error_YYYYMMDD.json
    """
    data_dir = ensure_dir((base_dir / cfg["output"]["data_dir"]).resolve())

    run_tag_date = date_th_compact()
    run_tag_datetime = datetime_th_compact()

    keep_run_manifest = cfg.get("output", {}).get("archive", {}).get("keep_run_manifest", True)
    keep_error_report = cfg.get("output", {}).get("archive", {}).get("keep_error_report", True)

    fred_cfg = cfg.get("fred", {}) or {}
    api_key = fred_cfg.get("api_key")
    observation_start = fred_cfg.get("observation_start", "2010-01-01")
    timeout_seconds = int(fred_cfg.get("timeout_seconds", 30))

    modes_cfg = fred_cfg.get("modes", {}) or {}
    run_modes = fred_cfg.get("run_modes")
    if run_modes is None:
        run_modes = [fred_cfg.get("run_mode", "daily")]
    if isinstance(run_modes, str):
        run_modes = [run_modes]

    attempts = int(cfg.get("retry", {}).get("attempts", 3))
    sleep_seconds = int(cfg.get("retry", {}).get("sleep_seconds", 2))

    overall_sources: Dict[str, Dict[str, Any]] = {}
    overall_stale: list[str] = []
    overall_notes: list[str] = []

    for mode in run_modes:
        series_ids = modes_cfg.get(mode, [])
        if not series_ids:
            logger.warning(f"No FRED series configured for mode '{mode}'. Skipping.")
            overall_notes.append(f"Mode {mode} has no series configured.")
            continue

        mode_dir = ensure_dir(data_dir / mode)
        manifest_path_latest = mode_dir / "fetch_manifest.json"
        manifest_path_archive = mode_dir / f"fetch_manifest_{run_tag_date}.json"
        error_path_archive = mode_dir / f"fetch_error_{run_tag_date}.json"
        output_path = mode_dir / f"{run_tag_datetime}.json"

        mode_sources: Dict[str, Dict[str, Any]] = {}
        mode_stale: list[str] = []
        error_items: list[Dict[str, str]] = []
        series_payload: Dict[str, list[Dict[str, Any]]] = {}

        for series_id in series_ids:
            status = SourceStatus(ok=False, rows=0, latest=None, used_cache=False, error=None)
            try:
                logger.info(f"[{mode}] Fetching FRED series {series_id} from {observation_start}...")
                df = retry(
                    lambda: fetch_fred_series_observations(
                        series_id=series_id,
                        api_key=api_key,
                        observation_start=observation_start,
                        timeout_seconds=timeout_seconds,
                    ),
                    attempts=attempts,
                    sleep_seconds=sleep_seconds,
                    logger=logger,
                    label=f"FRED_{series_id}",
                )

                if df.empty:
                    raise RuntimeError("FRED dataframe empty after fetch")

                latest_date = df["date"].iloc[-1].strftime("%Y-%m-%d")
                df["date"] = df["date"].dt.strftime("%Y-%m-%d")
                series_payload[series_id] = df.to_dict(orient="records")
                status = SourceStatus(ok=True, rows=len(df), latest=latest_date, used_cache=False, error=None)

                logger.info(f"[{mode}] Fetched {series_id} rows={len(df)} latest_date={latest_date}")

            except Exception as e:
                logger.error(f"[{mode}] Fetch FRED {series_id} failed: {e}")
                status = SourceStatus(ok=False, rows=0, latest=None, used_cache=False, error=str(e))
                error_items.append({"series_id": series_id, "error": str(e)})

            mode_sources[f"FRED_{series_id}"] = {**vars(status)}
            if status.used_cache:
                mode_stale.append(f"FRED_{series_id}")

        if series_payload:
            atomic_write_json(
                output_path,
                {
                    "asof_utc": utc_now_iso(),
                    "asof_th": thai_now_iso(),
                    "mode": mode,
                    "series": series_payload,
                },
            )
            logger.info(f"[{mode}] Saved snapshot: {output_path}")
        else:
            overall_notes.append(f"No data saved for mode {mode} (all series failed).")

        if error_items and keep_error_report:
            atomic_write_json(
                error_path_archive,
                {
                    "asof_utc": utc_now_iso(),
                    "asof_th": thai_now_iso(),
                    "mode": mode,
                    "errors": error_items,
                },
            )

        manifest = {
            "asof_utc": utc_now_iso(),
            "asof_th": thai_now_iso(),
            "sources": mode_sources,
            "stale_sources": mode_stale,
            "notes": "" if not error_items else f"FRED fetch failed for {len(error_items)} series.",
        }

        atomic_write_json(manifest_path_latest, manifest)
        if keep_run_manifest:
            atomic_write_json(manifest_path_archive, manifest)

        logger.info(f"[{mode}] Wrote manifest latest: {manifest_path_latest}")
        if keep_run_manifest:
            logger.info(f"[{mode}] Wrote manifest archive: {manifest_path_archive}")

        overall_sources.update({f"{mode}:{k}": v for k, v in mode_sources.items()})
        overall_stale.extend(mode_stale)
        if error_items:
            overall_notes.append(f"{mode} errors={len(error_items)}")

    overall_manifest = {
        "asof_utc": utc_now_iso(),
        "asof_th": thai_now_iso(),
        "sources": overall_sources,
        "stale_sources": overall_stale,
        "notes": "; ".join(note for note in overall_notes if note),
    }

    return overall_manifest
