# marketlab/research/evaluate.py
from __future__ import annotations
import pandas as pd
import numpy as np
from dataclasses import dataclass

@dataclass(frozen=True)
class EventStats:
    n: int
    mean: float
    std: float
    sharpe: float
    sharpe_ann: float
    q05: float
    q50: float
    q95: float
    hit_rate: float

def annualization_factor(timeframe: str, horizon: int) -> float:
    if timeframe == "1d":
        periods_per_year = 252 / horizon
    elif timeframe.endswith("h"):
        hours = int(timeframe[:-1])
        periods_per_year = (252 * 6.5) / hours / horizon
    else:
        periods_per_year = 252 / horizon  # fallback

    return np.sqrt(periods_per_year)

def summarize_returns(r: pd.Series, *, timeframe: str, horizon: int) -> EventStats:
    r = r.dropna()
    n = int(r.shape[0])
    if n == 0:
        return EventStats(0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)

    mean = float(r.mean())
    std = float(r.std(ddof=1)) if n > 1 else np.nan
    sharpe = float(mean / std) if std and std > 0 else np.nan

    ann_factor = annualization_factor(timeframe, horizon)
    sharpe_ann = float(sharpe * ann_factor) if np.isfinite(sharpe) else np.nan

    q05, q50, q95 = [float(x) for x in r.quantile([0.05, 0.50, 0.95])]
    hit_rate = float((r > 0).mean())

    return EventStats(n, mean, std, sharpe, sharpe_ann, q05, q50, q95, hit_rate)

def evaluate_event(
    df: pd.DataFrame,
    event_mask: pd.Series,
    fwd_ret: pd.Series,
    *,
    timeframe: str,
    horizon: int,
    ) -> pd.DataFrame:
    if not event_mask.index.equals(df.index) or not fwd_ret.index.equals(df.index):
        raise ValueError("Indices must match")

    unconditional = summarize_returns(fwd_ret, timeframe=timeframe, horizon=horizon)
    conditional = summarize_returns(fwd_ret[event_mask], timeframe=timeframe, horizon=horizon)

    out = pd.DataFrame(
        [
            {"slice": "unconditional", **unconditional.__dict__},
            {"slice": "conditional", **conditional.__dict__},
        ]
    )
    return out
