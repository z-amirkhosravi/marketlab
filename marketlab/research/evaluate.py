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
    q05: float
    q50: float
    q95: float
    hit_rate: float

def summarize_returns(r: pd.Series) -> EventStats:
    r = r.dropna()
    n = int(r.shape[0])
    if n == 0:
        return EventStats(0, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)
    mean = float(r.mean())
    std = float(r.std(ddof=1)) if n > 1 else np.nan
    sharpe = float(mean / std) if std and std > 0 else np.nan
    q05, q50, q95 = [float(x) for x in r.quantile([0.05, 0.50, 0.95])]
    hit_rate = float((r > 0).mean())
    return EventStats(n, mean, std, sharpe, q05, q50, q95, hit_rate)

def evaluate_event(
    df: pd.DataFrame,
    event_mask: pd.Series,
    fwd_ret: pd.Series,
) -> pd.DataFrame:
    """
    Returns a tiny table comparing unconditional vs conditional on event.
    """
    if not event_mask.index.equals(df.index) or not fwd_ret.index.equals(df.index):
        raise ValueError("Indices must match")

    unconditional = summarize_returns(fwd_ret)
    conditional = summarize_returns(fwd_ret[event_mask])

    out = pd.DataFrame(
        [
            {"slice": "unconditional", **unconditional.__dict__},
            {"slice": "conditional", **conditional.__dict__},
        ]
    )
    return out
