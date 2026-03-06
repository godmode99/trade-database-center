from __future__ import annotations

import requests
import pandas as pd
from typing import Optional


def fetch_fred_series_observations(
    series_id: str,
    api_key: Optional[str],
    observation_start: str,
    timeout_seconds: int = 30,
) -> pd.DataFrame:
    """
    Returns DataFrame columns: date, value
    """
    key = api_key or ""
    params = {
        "series_id": series_id,
        "file_type": "json",
        "observation_start": observation_start,
    }
    if key:
        params["api_key"] = key

    url = "https://api.stlouisfed.org/fred/series/observations"
    r = requests.get(url, params=params, timeout=timeout_seconds)
    r.raise_for_status()
    data = r.json()

    obs = data.get("observations", [])
    if not obs:
        raise RuntimeError(f"No observations returned for series_id={series_id}")

    df = pd.DataFrame(obs)
    # df has: date, value (value can be ".")
    df = df[["date", "value"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"].replace(".", pd.NA), errors="coerce")

    # drop rows where value missing (FRED returns "." sometimes)
    df = df.dropna(subset=["value"]).reset_index(drop=True)
    df = df.sort_values("date").reset_index(drop=True)

    return df
