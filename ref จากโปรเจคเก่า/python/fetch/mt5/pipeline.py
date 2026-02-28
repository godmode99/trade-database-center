# pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Callable

import pandas as pd

from fetch_mt5 import MT5Client
from utils import (
    ensure_dir,
    th_now_iso,
    atomic_write_json,
    date_th_compact,
    timestamp_th_compact,
    timestamp_th_compact_with_t,
    build_output_filename,
    build_feature_filename,
    save_json,
    load_cache_json,
    find_latest_cache,
)
from features import compute_features, select_feature_columns
from zoneinfo import ZoneInfo


TH_TZ = ZoneInfo("Asia/Bangkok")

TIMEFRAME_RANK = {
    "MN1": 600,
    "W1": 500,
    "D1": 400,
    "H12": 300,
    "H8": 290,
    "H6": 280,
    "H4": 270,
    "H2": 260,
    "H1": 250,
    "M30": 200,
    "M15": 190,
    "M5": 180,
    "M1": 170,
}


def _safe_float(value: Any, decimals: int | None = None) -> float | None:
    if value is None or pd.isna(value):
        return None
    out = float(value)
    if decimals is not None:
        out = round(out, decimals)
    return out


def _safe_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _ensure_th(ts: pd.Timestamp) -> pd.Timestamp:
    if ts.tzinfo is None:
        return ts.tz_localize(TH_TZ)
    return ts.tz_convert(TH_TZ)


def _time_iso_th(ts: pd.Timestamp) -> str:
    return _ensure_th(ts).isoformat(timespec="seconds")


def _time_iso_utc(ts: pd.Timestamp) -> str:
    return _ensure_th(ts).tz_convert("UTC").isoformat(timespec="seconds").replace("+00:00", "Z")


def _trend_hint_from_event(event_type: str | None) -> str | None:
    if not event_type:
        return None
    return {
        "BOS_UP": "bullish_or_range",
        "BOS_DN": "bearish_or_range",
        "CHOCH_UP": "bullish_reversal",
        "CHOCH_DN": "bearish_reversal",
    }.get(event_type, None)


def _format_last_event(
    features_df: pd.DataFrame,
    event_type: str,
    idx: int,
    asof_index: int,
) -> Dict[str, Any]:
    idx_pos = int(features_df.index.get_loc(idx))
    age_bars = max(0, asof_index - idx_pos)
    return {
        "type": event_type,
        "time_th": _time_iso_th(pd.Timestamp(features_df.loc[idx, "time_th"])),
        "level_close": _safe_float(features_df.loc[idx, "close"]),
        "age_bars": age_bars,
    }

def _age_days(asof_time: pd.Timestamp, event_time: pd.Timestamp) -> int:
    delta_days = (asof_time - event_time).total_seconds() / 86400
    return max(0, int(delta_days))


def _collect_swings_recent(
    features_df: pd.DataFrame,
    flag_col: str,
    price_col: str,
    asof_time: pd.Timestamp,
    window_days: int,
    price_filter: Callable[[float], bool] | None = None,
) -> List[Dict[str, Any]]:
    start_time = asof_time - pd.Timedelta(days=window_days)
    swings = features_df.loc[
        (features_df[flag_col] == 1) & (features_df["time_th"] >= start_time),
        ["time_th", price_col],
    ]
    payload = []
    for _, row in swings.iterrows():
        price = _safe_float(row[price_col])
        if price is None:
            continue
        if price_filter and not price_filter(price):
            continue
        payload.append(
            {
                "time_th": _time_iso_th(pd.Timestamp(row["time_th"])),
                "price": price,
            }
        )
    return payload


def _collect_swings_nearest(
    features_df: pd.DataFrame,
    flag_col: str,
    price_col: str,
    asof_time: pd.Timestamp,
    price_now: float,
    atr: float,
    max_distance_atr: float,
    price_filter: Callable[[float], bool] | None = None,
) -> List[Dict[str, Any]]:
    swings = features_df.loc[features_df[flag_col] == 1, ["time_th", price_col]]
    payload = []
    for _, row in swings.iterrows():
        price = _safe_float(row[price_col])
        if price is None:
            continue
        if price_filter and not price_filter(price):
            continue
        distance_atr = abs(price - price_now) / atr
        if distance_atr > max_distance_atr:
            continue
        time_th = pd.Timestamp(row["time_th"])
        payload.append(
            {
                "time_th": _time_iso_th(time_th),
                "price": price,
                "distance_atr": distance_atr,
            }
        )
    payload.sort(key=lambda item: item["distance_atr"])
    return payload


def _build_positioning_atr(last_row: pd.Series, prev_levels: Dict[str, Any]) -> Dict[str, Any] | None:
    atr = _safe_float(last_row.get("atr14"))
    price_now = _safe_float(last_row.get("close"))
    if atr is None or atr == 0 or price_now is None:
        return None
    payload: Dict[str, Any] = {"price_now": price_now}
    for level_key, level_value in prev_levels.items():
        if level_value is None:
            continue
        level_label = str(level_key).upper()
        payload[f"dist_to_{level_label}_atr"] = _safe_float(abs(float(level_value) - price_now) / atr, 4)
    ema20 = _safe_float(last_row.get("ema20"))
    if ema20 is not None:
        payload["dist_to_ema20_atr"] = _safe_float(abs(ema20 - price_now) / atr, 4)
    ema50 = _safe_float(last_row.get("ema50"))
    if ema50 is not None:
        payload["dist_to_ema50_atr"] = _safe_float(abs(ema50 - price_now) / atr, 4)
    return payload


def _summary_timeframe_payload(
    timeframe: str,
    raw_df: pd.DataFrame,
    features_df: pd.DataFrame,
    bars: int,
    prev_period: str | None,
) -> Dict[str, Any]:
    last_raw = raw_df.iloc[-1]
    last_features = features_df.iloc[-1]
    asof_time = pd.Timestamp(last_raw["time_th"])
    asof_index = len(features_df) - 1

    last_bar = {
        "time_th": _time_iso_th(pd.Timestamp(last_raw["time_th"])),
        "open": _safe_float(last_raw.get("open")),
        "high": _safe_float(last_raw.get("high")),
        "low": _safe_float(last_raw.get("low")),
        "close": _safe_float(last_raw.get("close")),
        "tick_volume": _safe_int(last_raw.get("tick_volume")),
    }

    indicators = {
        "atr14": _safe_float(last_features.get("atr14")),
        "ema20": _safe_float(last_features.get("ema20")),
        "ema50": _safe_float(last_features.get("ema50")),
    }

    events = features_df["structure_event"].dropna()
    last_event = None
    if not events.empty:
        idx = events.index[-1]
        last_event = _format_last_event(features_df, str(events.iloc[-1]), idx, asof_index)

    bos_events = events[events.str.startswith("BOS")]
    last_bos = None
    if not bos_events.empty:
        idx = bos_events.index[-1]
        last_bos = _format_last_event(features_df, str(bos_events.iloc[-1]), idx, asof_index)

    choch_events = events[events.str.startswith("CHOCH")]
    last_choch = None
    if not choch_events.empty:
        idx = choch_events.index[-1]
        last_choch = _format_last_event(features_df, str(choch_events.iloc[-1]), idx, asof_index)

    structure = {
        "last_event": last_event,
        "last_bos": last_bos,
        "last_choch": last_choch,
    }

    prev_levels = {}
    for key in ["pdh", "pdl", "pdc", "pwh", "pwl", "pwc", "pmh", "pml", "pmc"]:
        if key in last_features:
            prev_levels[key] = _safe_float(last_features.get(key))
    prev_levels = {k: v for k, v in prev_levels.items() if v is not None}

    positioning = _build_positioning_atr(last_features, prev_levels)

    notes_flags = {
        "sweep_prev_high": _safe_int(last_features.get("sweep_prev_high")) or 0,
        "sweep_prev_low": _safe_int(last_features.get("sweep_prev_low")) or 0,
    }

    swing_window_map = {"D1": 120, "H4": 30}
    window_days = swing_window_map.get(timeframe, 60)
    max_distance_atr = 3.0
    max_ranked_levels = 15
    atr = _safe_float(last_features.get("atr14"))
    price_now = _safe_float(last_features.get("close"))

    swings_recent = {
        "window_days": window_days,
        "highs": _collect_swings_recent(features_df, "swing_high", "high", asof_time, window_days),
        "lows": _collect_swings_recent(features_df, "swing_low", "low", asof_time, window_days),
    }

    swings_nearest = None
    key_levels_ranked_far: List[Dict[str, Any]] = []
    if atr and price_now:
        swings_nearest = {
            "max_distance_atr": max_distance_atr,
            "resistance_highs_nearest": _collect_swings_nearest(
                features_df,
                "swing_high",
                "high",
                asof_time,
                price_now,
                atr,
                max_distance_atr,
                price_filter=lambda price: price >= price_now,
            ),
            "support_lows_nearest": _collect_swings_nearest(
                features_df,
                "swing_low",
                "low",
                asof_time,
                price_now,
                atr,
                max_distance_atr,
                price_filter=lambda price: price <= price_now,
            ),
        }
        swings_recent["resistance_highs_recent"] = _collect_swings_recent(
            features_df,
            "swing_high",
            "high",
            asof_time,
            window_days,
            price_filter=lambda price: price >= price_now,
        )
        swings_recent["support_lows_recent"] = _collect_swings_recent(
            features_df,
            "swing_low",
            "low",
            asof_time,
            window_days,
            price_filter=lambda price: price <= price_now,
        )

    key_levels_ranked = []
    if atr and price_now:
        recency_distance_atr = 1.5
        ema20 = _safe_float(last_features.get("ema20"))
        if ema20 is not None:
            key_levels_ranked.append(
                {
                    "type": "EMA20",
                    "level": ema20,
                    "distance_atr": abs(ema20 - price_now) / atr,
                    "age_days": 0,
                }
            )
        ema50 = _safe_float(last_features.get("ema50"))
        if ema50 is not None:
            key_levels_ranked.append(
                {
                    "type": "EMA50",
                    "level": ema50,
                    "distance_atr": abs(ema50 - price_now) / atr,
                    "age_days": 0,
                }
            )
        for level_key, level_value in prev_levels.items():
            if level_value is None:
                continue
            age_days = 0
            if prev_period:
                period_days = {"D": 1, "W": 7, "M": 30}.get(prev_period.upper(), 0)
                age_days = period_days
            key_levels_ranked.append(
                {
                    "type": str(level_key).upper(),
                    "level": _safe_float(level_value),
                    "distance_atr": abs(float(level_value) - price_now) / atr,
                    "age_days": age_days,
                }
            )

        for event in [last_bos, last_choch]:
            if not event:
                continue
            event_time = pd.Timestamp(event["time_th"])
            key_levels_ranked.append(
                {
                    "type": event["type"],
                    "level": event["level_close"],
                    "distance_atr": abs(float(event["level_close"]) - price_now) / atr,
                    "age_days": _age_days(asof_time, event_time),
                }
            )

        for flag_col, price_col, level_type in [
            ("swing_high", "high", "SWING_HIGH"),
            ("swing_low", "low", "SWING_LOW"),
        ]:
            swings = features_df.loc[features_df[flag_col] == 1, ["time_th", price_col]]
            for _, row in swings.iterrows():
                price = _safe_float(row[price_col])
                if price is None:
                    continue
                if level_type == "SWING_HIGH" and price < price_now:
                    continue
                if level_type == "SWING_LOW" and price > price_now:
                    continue
                time_th = pd.Timestamp(row["time_th"])
                age_days = _age_days(asof_time, time_th)
                distance_atr = abs(price - price_now) / atr
                if age_days > window_days and distance_atr > recency_distance_atr:
                    continue
                key_levels_ranked.append(
                    {
                        "type": level_type,
                        "level": price,
                        "distance_atr": distance_atr,
                        "age_days": age_days,
                    }
                )

        key_levels_ranked.sort(key=lambda item: (item["distance_atr"], item["age_days"]))
        key_levels_ranked_near = [
            item
            for item in key_levels_ranked
            if item["distance_atr"] <= max_distance_atr and item["age_days"] <= window_days
        ][:max_ranked_levels]
        key_levels_ranked_far = [
            item
            for item in key_levels_ranked
            if item["distance_atr"] > max_distance_atr and item["age_days"] <= window_days
        ][:max_ranked_levels]
        key_levels_ranked = key_levels_ranked_near

    payload = {
        "lookback_bars": bars,
        "last_bar": last_bar,
        "indicators": indicators,
        "structure": structure,
    }
    if prev_levels:
        payload["prev_levels"] = prev_levels
    if any(
        value
        for key, value in swings_recent.items()
        if key != "window_days"
    ):
        payload["swings_recent"] = swings_recent
    if swings_nearest and (swings_nearest["resistance_highs_nearest"] or swings_nearest["support_lows_nearest"]):
        payload["swings_nearest"] = swings_nearest
    if positioning:
        payload["positioning_atr"] = positioning
    if key_levels_ranked:
        payload["key_levels_ranked"] = key_levels_ranked
    if key_levels_ranked_far:
        payload["key_levels_ranked_far"] = key_levels_ranked_far
    payload["notes_flags"] = notes_flags
    return payload


def build_bias_summary(
    symbol: str,
    raw_frames: Dict[str, pd.DataFrame],
    feature_timeframes: Dict[str, Dict[str, Any]],
    feature_files: Dict[str, str],
    pivot_left: int,
    pivot_right: int,
    bars_lookup: Dict[str, int],
    timezone: str,
) -> Dict[str, Any] | None:
    timeframes_payload: Dict[str, Any] = {}
    asof_time: pd.Timestamp | None = None
    asof_rank = -1

    for timeframe, feature_item in feature_timeframes.items():
        raw_df = raw_frames.get(timeframe)
        if raw_df is None or raw_df.empty:
            continue
        prev_period = feature_item.get("prev_period")
        features_df = compute_features(raw_df, pivot_left, pivot_right, prev_period=prev_period)
        timeframes_payload[timeframe] = _summary_timeframe_payload(
            timeframe,
            raw_df,
            features_df,
            bars_lookup.get(timeframe, len(raw_df)),
            prev_period,
        )

        rank = TIMEFRAME_RANK.get(timeframe, 0)
        last_time = pd.Timestamp(raw_df.iloc[-1]["time_th"])
        if rank > asof_rank:
            asof_rank = rank
            asof_time = last_time

    if not timeframes_payload:
        return None

    if asof_time is None:
        asof_time = pd.Timestamp(list(raw_frames.values())[0].iloc[-1]["time_th"])

    asof = {
        "time_th": _time_iso_th(asof_time),
        "time_utc": _time_iso_utc(asof_time),
    }

    inputs: Dict[str, Any] = {}
    for timeframe, filename in feature_files.items():
        inputs[f"{timeframe.lower()}_features_file"] = filename

    summary = {
        "schema_version": "bias_summary.v1",
        "symbol": symbol,
        "asof": asof,
        "meta": {
            "timezone_for_period_levels": timezone,
            "inputs": inputs,
            "feature_rules": {
                "recency_filter": "Levels older than window are only kept if distance_atr <= 1.5 (nearest swings).",
                "rank_rule": "Sort by distance_atr then age_days; key_levels_ranked keeps distance_atr <= 3 and age_days <= window_days.",
            },
            "atr_period": 14,
            "atr_method": "EMA_TR (alpha=1/14, adjust=False)",
            "swing_pivot": {"left": pivot_left, "right": pivot_right},
            "bos_rule": "close_break (based on your implementation)",
            "choch_rule": "close_break (based on your implementation)",
        },
        "timeframes": timeframes_payload,
        "calendar_risk": {
            "next_high_impact_utc": None,
            "rule": "avoid_new_entries_60m_before_high_impact",
        },
    }
    return summary


@dataclass
class SourceStatus:
    ok: bool
    rows: int
    latest_time: str | None
    used_cache: bool
    error: str | None


def validate_ohlc(df: pd.DataFrame, cfg: Dict[str, Any]) -> None:
    if df.empty:
        raise ValueError("OHLC dataframe is empty")

    min_price = float(cfg["validation"]["min_price"])
    max_price = float(cfg["validation"]["max_price"])
    max_missing_ratio = float(cfg["validation"]["max_missing_ratio"])

    for c in ["open", "high", "low", "close"]:
        miss = float(df[c].isna().mean())
        if miss > max_missing_ratio:
            raise ValueError(f"Too many missing values in {c}: {miss:.4f} > {max_missing_ratio}")
        if (df[c] <= 0).any():
            raise ValueError(f"Non-positive prices in {c}")
        if (df[c] < min_price).any() or (df[c] > max_price).any():
            raise ValueError(f"Price out of range in {c} (expected {min_price}..{max_price})")

    # OHLC containment
    if not ((df["low"] <= df["open"]) & (df["open"] <= df["high"])).all():
        raise ValueError("OHLC containment failed for open")
    if not ((df["low"] <= df["close"]) & (df["close"] <= df["high"])).all():
        raise ValueError("OHLC containment failed for close")

    # time monotonic
    if not df["time_th"].is_monotonic_increasing:
        raise ValueError("time_th is not sorted increasing")


def load_cache_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if "time_th" in df.columns:
        df["time_th"] = pd.to_datetime(df["time_th"])
    elif "time_utc" in df.columns:
        df["time_th"] = pd.to_datetime(df["time_utc"], utc=True).dt.tz_convert("Asia/Bangkok")
        df = df.drop(columns=["time_utc"])
    return df


def save_csv(df: pd.DataFrame, path: Path) -> None:
    """
    Always overwrites OHLC files (as requested). We store time_th as ISO-TH string.
    """
    out = df.copy()
    out["time_th"] = out["time_th"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    out.to_csv(path, index=False)


def save_feature_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    if "time_th" in out.columns:
        out["time_th"] = out["time_th"].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    out.to_csv(path, index=False)


def format_timeframe_label(timeframe: str) -> str:
    tf = timeframe.upper()
    digits = "".join(ch for ch in tf if ch.isdigit())
    letters = "".join(ch for ch in tf if ch.isalpha())
    if letters == "MN":
        letters = "M"
    return f"{digits}{letters}" if digits and letters else tf


def run_fetch_pipeline(cfg: Dict[str, Any], logger, base_dir: Path) -> Dict[str, Any]:
    """
    Policy:
      - OHLC CSVs overwrite (eurusd_d1.csv, eurusd_h4.csv)
      - Latest manifest overwrites (fetch_manifest.json)
      - Run manifest archived with date suffix (fetch_manifest_YYYYMMDD.json)
      - Error report archived with date suffix on failures (fetch_error_YYYYMMDD.json)
    """
    # Resolve output dirs relative to the folder containing main.py/config.yaml
    data_dir = ensure_dir((base_dir / cfg["output"]["data_dir"]).resolve())

    run_tag = date_th_compact()  # YYYYMMDD (TH)

    # Manifests
    manifest_path_latest = data_dir / "fetch_manifest.json"                 # overwrite
    manifest_path_archive = data_dir / f"fetch_manifest_{run_tag}.json"     # keep
    error_path_archive = data_dir / f"fetch_error_{run_tag}.json"           # keep on failure

    keep_run_manifest = cfg.get("archive", {}).get("keep_run_manifest", True)
    keep_error_report = cfg.get("archive", {}).get("keep_error_report", True)

    terminal_path = cfg["mt5"].get("terminal_path") or None
    symbols: List[str] = cfg.get("symbols", ["EURUSD"])
    fetch_cfg = cfg.get("fetch", {}) or {}
    store_time_as_th_default = bool(fetch_cfg.get("store_time_as_th", True))
    feature_cfg = cfg.get("features", {}) or {}
    output_format = str(cfg.get("output", {}).get("format", "csv")).lower()
    if output_format == "cvs":
        output_format = "csv"
    file_label_default = str(cfg.get("output", {}).get("file_label", "data"))
    timeframe_configs = fetch_cfg.get("timeframes")
    if timeframe_configs:
        fetch_specs = [
            {
                "timeframe": str(item["timeframe"]).upper(),
                "bars": int(item["bars"]),
                "store_time_as_th": bool(item.get("store_time_as_th", store_time_as_th_default)),
                "file_label": str(item.get("file_label", file_label_default)),
                "feature_label": str(item.get("feature_label", str(feature_cfg.get("file_label", "feature")))),
            }
            for item in timeframe_configs
        ]
    else:
        fetch_specs = [
            {
                "timeframe": str(fetch_cfg["timeframe"]).upper(),
                "bars": int(fetch_cfg["bars"]),
                "store_time_as_th": store_time_as_th_default,
                "file_label": file_label_default,
                "feature_label": str(feature_cfg.get("file_label", "feature")),
            }
        ]

    pivot_left = int(feature_cfg.get("pivot_left", 2))
    pivot_right = int(feature_cfg.get("pivot_right", 2))
    feature_timeframes = {
        str(item.get("timeframe", "")).upper(): item for item in feature_cfg.get("timeframes", []) if item.get("timeframe")
    }

    mt5c = MT5Client(terminal_path=terminal_path)
    stale_sources: List[str] = []
    statuses: Dict[str, SourceStatus] = {}
    raw_frames: Dict[str, Dict[str, pd.DataFrame]] = {sym: {} for sym in symbols}
    feature_files_by_symbol: Dict[str, Dict[str, str]] = {sym: {} for sym in symbols}

    # --- CONNECT ---
    try:
        logger.info("Connecting to MT5...")
        mt5c.connect()
        logger.info("MT5 connected.")
    except Exception as e:
        logger.error(f"MT5 connect failed: {e}")
        try:
            mt5c.shutdown()
        except Exception:
            pass

        # Fallback to cache for each symbol/timeframe
        for sym in symbols:
            for spec in fetch_specs:
                timeframe = spec["timeframe"]
                file_label = spec["file_label"]
                timeframe_label = format_timeframe_label(timeframe)
                cache_path = find_latest_cache(data_dir, sym, file_label, output_format, timeframe_label)
                if cache_path and output_format == "json":
                    cache_df = load_cache_json(cache_path)
                elif cache_path:
                    cache_df = load_cache_csv(cache_path)
                else:
                    cache_df = None
                key = f"{sym}_{timeframe}"
                if cache_df is not None and len(cache_df) > 0:
                    latest = pd.to_datetime(cache_df["time_th"].iloc[-1]).strftime("%Y-%m-%dT%H:%M:%S%z")
                    statuses[key] = SourceStatus(ok=True, rows=len(cache_df), latest_time=latest, used_cache=True, error=str(e))
                    stale_sources.append(key)
                else:
                    statuses[key] = SourceStatus(ok=False, rows=0, latest_time=None, used_cache=False, error=str(e))

        manifest = {
            "asof_th": th_now_iso(),
            "sources": {k: vars(v) for k, v in statuses.items()},
            "stale_sources": stale_sources,
            "notes": "MT5 connect failed; used cache where available.",
        }

        # Write latest + archive manifest
        atomic_write_json(manifest_path_latest, manifest)
        if keep_run_manifest:
            atomic_write_json(manifest_path_archive, manifest)

        # Write error report (dated)
        if keep_error_report:
            atomic_write_json(error_path_archive, {
                "asof_th": th_now_iso(),
                "stage": "connect_mt5",
                "error": str(e),
            })

        return manifest

    # --- FETCH ---
    for sym in symbols:
        for spec in fetch_specs:
            timeframe = spec["timeframe"]
            bars = spec["bars"]
            store_time_as_th = spec["store_time_as_th"]
            file_label = spec["file_label"]
            feature_label = spec["feature_label"]
            timeframe_label = format_timeframe_label(timeframe)
            timestamp = timestamp_th_compact()
            filename = build_output_filename(sym, file_label, output_format, timestamp, timeframe_label)
            output_path = data_dir / filename
            try:
                logger.info(f"Fetching {sym} {timeframe} ({bars} bars)...")
                res = mt5c.fetch_rates(sym, timeframe, bars, store_time_as_th=store_time_as_th)
                validate_ohlc(res.df, cfg)
                if output_format == "json":
                    save_json(res.df, output_path)
                else:
                    save_csv(res.df, output_path)
                raw_frames[sym][timeframe] = res.df
                feature_item = feature_timeframes.get(timeframe, {})
                feature_columns = feature_item.get("columns")
                if feature_columns:
                    prev_period = feature_item.get("prev_period")
                    features_df = compute_features(res.df, pivot_left, pivot_right, prev_period=prev_period)
                    selected = select_feature_columns(features_df, feature_columns)
                    feature_filename = build_feature_filename(sym, feature_label, output_format, timestamp, timeframe_label)
                    feature_path = data_dir / feature_filename
                    if output_format == "json":
                        save_json(selected, feature_path)
                    else:
                        save_feature_csv(selected, feature_path)
                    logger.info(f"Saved {feature_path} rows={len(selected)}")
                    feature_files_by_symbol[sym][timeframe] = feature_filename
                statuses[f"{sym}_{timeframe}"] = SourceStatus(
                    ok=True,
                    rows=res.rows,
                    latest_time=res.latest_time_th,
                    used_cache=False,
                    error=None,
                )
                logger.info(f"Saved {output_path} rows={res.rows} latest={res.latest_time_th}")
            except Exception as e:
                logger.error(f"Fetch {sym} {timeframe} failed: {e}")
                cache_path = find_latest_cache(data_dir, sym, file_label, output_format, timeframe_label)
                if cache_path and output_format == "json":
                    cache_df = load_cache_json(cache_path)
                elif cache_path:
                    cache_df = load_cache_csv(cache_path)
                else:
                    cache_df = None
                key = f"{sym}_{timeframe}"
                if cache_df is not None and len(cache_df) > 0:
                    latest = pd.to_datetime(cache_df["time_th"].iloc[-1]).strftime("%Y-%m-%dT%H:%M:%S%z")
                    statuses[key] = SourceStatus(ok=True, rows=len(cache_df), latest_time=latest, used_cache=True, error=str(e))
                    stale_sources.append(key)
                    logger.warning(f"Using cache for {sym} {timeframe} (stale).")
                    raw_frames[sym][timeframe] = cache_df
                    if keep_error_report:
                        atomic_write_json(error_path_archive, {
                            "asof_th": th_now_iso(),
                            "stage": f"fetch_{sym}_{timeframe}",
                            "error": str(e),
                        })
                else:
                    statuses[key] = SourceStatus(ok=False, rows=0, latest_time=None, used_cache=False, error=str(e))
                    if keep_error_report:
                        atomic_write_json(error_path_archive, {
                            "asof_th": th_now_iso(),
                            "stage": f"fetch_{sym}_{timeframe}",
                            "error": str(e),
                        })

    # Shutdown MT5 cleanly
    try:
        mt5c.shutdown()
    except Exception:
        pass

    # --- MANIFEST WRITE ---
    manifest = {
        "asof_th": th_now_iso(),
        "sources": {k: vars(v) for k, v in statuses.items()},
        "stale_sources": stale_sources,
        "notes": "",
    }

    # Always overwrite latest manifest
    atomic_write_json(manifest_path_latest, manifest)
    # Archive manifest with date suffix
    if keep_run_manifest:
        atomic_write_json(manifest_path_archive, manifest)

    logger.info(f"Wrote manifest latest: {manifest_path_latest}")
    if keep_run_manifest:
        logger.info(f"Wrote manifest archive: {manifest_path_archive}")

    summary_cfg = cfg.get("summary", {}) or {}
    summary_enabled = bool(summary_cfg.get("enabled", True))
    if summary_enabled:
        summary_label = str(summary_cfg.get("file_label", f"{file_label_default}"))
        summary_timestamp = timestamp_th_compact_with_t()
        timezone = str(cfg.get("app", {}).get("timezone", "Asia/Bangkok"))
        bars_lookup = {spec["timeframe"]: spec["bars"] for spec in fetch_specs}
        for sym in symbols:
            summary_payload = build_bias_summary(
                sym,
                raw_frames.get(sym, {}),
                feature_timeframes,
                feature_files_by_symbol.get(sym, {}),
                pivot_left,
                pivot_right,
                bars_lookup,
                timezone,
            )
            if not summary_payload:
                continue
            suffix = f"_{sym.lower()}" if len(symbols) > 1 else ""
            summary_name = f"{summary_label}_summary{suffix}_{summary_timestamp}.json"
            summary_path = data_dir / summary_name
            atomic_write_json(summary_path, summary_payload)
            logger.info(f"Wrote summary: {summary_path}")

    return manifest
