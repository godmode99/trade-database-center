from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd


@dataclass
class FeatureSpec:
    timeframe: str
    columns: Sequence[str]
    prev_period: str | None = None


CORE_FEATURES = [
    "tr",
    "atr14",
    "range",
    "body",
    "upper_wick",
    "lower_wick",
    "close_pos",
    "swing_high",
    "swing_low",
    "structure_event",
]


OPTIONAL_FEATURES = [
    "ema20",
    "ema50",
    "pdh",
    "pdl",
    "pdc",
    "pwh",
    "pwl",
    "pwc",
    "pmh",
    "pml",
    "pmc",
    "sweep_prev_high",
    "sweep_prev_low",
    "bos_up",
    "bos_dn",
    "choch_up",
    "choch_dn",
]


def _safe_close_pos(df: pd.DataFrame) -> pd.Series:
    rng = df["high"] - df["low"]
    pos = (df["close"] - df["low"]) / rng.replace(0, pd.NA)
    return pos.fillna(0.0)


def _swing_flags(df: pd.DataFrame, left: int, right: int) -> tuple[pd.Series, pd.Series]:
    highs = df["high"].to_numpy()
    lows = df["low"].to_numpy()
    size = len(df)
    swing_high = [0] * size
    swing_low = [0] * size
    for idx in range(size):
        if idx < left or idx + right >= size:
            continue
        high_window = highs[idx - left: idx + right + 1]
        low_window = lows[idx - left: idx + right + 1]
        current_high = highs[idx]
        current_low = lows[idx]
        if current_high == high_window.max() and current_high > high_window[:left].max() and current_high > high_window[left + 1:].max():
            swing_high[idx] = 1
        if current_low == low_window.min() and current_low < low_window[:left].min() and current_low < low_window[left + 1:].min():
            swing_low[idx] = 1
    return pd.Series(swing_high, index=df.index), pd.Series(swing_low, index=df.index)


def _structure_events(df: pd.DataFrame) -> pd.DataFrame:
    events: list[str | None] = [None] * len(df)
    bos_up = [0] * len(df)
    bos_dn = [0] * len(df)
    choch_up = [0] * len(df)
    choch_dn = [0] * len(df)

    last_swing_high = None
    last_swing_low = None
    trend = None

    for i, row in df.iterrows():
        if row["swing_high"] == 1:
            last_swing_high = row["high"]
        if row["swing_low"] == 1:
            last_swing_low = row["low"]

        close = row["close"]
        event = None
        if last_swing_high is not None and close > last_swing_high:
            if trend == "down":
                event = "CHOCH_UP"
                choch_up[i] = 1
            else:
                event = "BOS_UP"
                bos_up[i] = 1
            trend = "up"
        elif last_swing_low is not None and close < last_swing_low:
            if trend == "up":
                event = "CHOCH_DN"
                choch_dn[i] = 1
            else:
                event = "BOS_DN"
                bos_dn[i] = 1
            trend = "down"

        events[i] = event

    return pd.DataFrame(
        {
            "structure_event": events,
            "bos_up": bos_up,
            "bos_dn": bos_dn,
            "choch_up": choch_up,
            "choch_dn": choch_dn,
        },
        index=df.index,
    )


def _prev_period_levels(df: pd.DataFrame, period: str) -> pd.DataFrame:
    period = period.upper()
    freq_map = {"D": "D", "W": "W-MON", "M": "M"}
    if period not in freq_map:
        raise ValueError(f"Unsupported prev_period: {period}. Use D, W, or M.")

    period_key = df["time_th"].dt.to_period(freq_map[period])
    grouped = df.groupby(period_key).agg(high=("high", "max"), low=("low", "min"), close=("close", "last"))
    shifted = grouped.shift(1)
    prev = shifted.reindex(period_key).reset_index(drop=True)

    prefix_map = {"D": "pd", "W": "pw", "M": "pm"}
    prefix = prefix_map[period]
    prev = prev.rename(
        columns={
            "high": f"{prefix}h",
            "low": f"{prefix}l",
            "close": f"{prefix}c",
        }
    )
    return prev


def compute_features(
    df: pd.DataFrame,
    pivot_left: int,
    pivot_right: int,
    prev_period: str | None = None,
) -> pd.DataFrame:
    out = df.copy()
    prev_close = out["close"].shift(1)
    range_ = out["high"] - out["low"]
    tr = pd.concat(
        [
            range_,
            (out["high"] - prev_close).abs(),
            (out["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    out["tr"] = tr
    out["atr14"] = tr.ewm(alpha=1 / 14, adjust=False, min_periods=1).mean()
    out["range"] = range_
    out["body"] = (out["close"] - out["open"]).abs()
    out["upper_wick"] = out["high"] - out[["open", "close"]].max(axis=1)
    out["lower_wick"] = out[["open", "close"]].min(axis=1) - out["low"]
    out["close_pos"] = _safe_close_pos(out)

    swing_high, swing_low = _swing_flags(out, pivot_left, pivot_right)
    out["swing_high"] = swing_high
    out["swing_low"] = swing_low

    structure = _structure_events(out)
    out = pd.concat([out, structure], axis=1)

    out["ema20"] = out["close"].ewm(span=20, adjust=False).mean()
    out["ema50"] = out["close"].ewm(span=50, adjust=False).mean()

    if prev_period:
        prev_levels = _prev_period_levels(out, prev_period)
        out = pd.concat([out.reset_index(drop=True), prev_levels], axis=1)
        out["sweep_prev_high"] = ((out["high"] > prev_levels.iloc[:, 0]) & (out["close"] < prev_levels.iloc[:, 0])).astype(int)
        out["sweep_prev_low"] = ((out["low"] < prev_levels.iloc[:, 1]) & (out["close"] > prev_levels.iloc[:, 1])).astype(int)
    else:
        out["sweep_prev_high"] = 0
        out["sweep_prev_low"] = 0

    return out


def select_feature_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    base_cols = ["time_th", "open", "high", "low", "close", "tick_volume"]
    requested = [col for col in columns if col in df.columns]
    ordered = []
    for col in base_cols + requested:
        if col in df.columns and col not in ordered:
            ordered.append(col)
    return df[ordered]
