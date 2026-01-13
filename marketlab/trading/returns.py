from __future__ import annotations
import pandas as pd
import numpy as np

from marketlab.trading.signals import TradeSignal

def trade_returns_next_open_close_at_horizon(
    df: pd.DataFrame,
    *,
    horizon: int,
    signal: TradeSignal,
    cost_bps: float = 0.0,
) -> pd.Series:
    """
    Return series aligned to the *signal day* t:
      - enter at open of t+1
      - exit at close of t+horizon
    For horizon=1: enter next open, exit same day's close.

    Returns a Series indexed like df.index.
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")

    entry = df["open"].shift(-1)
    exit_ = df["close"].shift(-horizon)

    r = (exit_ / entry) - 1.0
    r = r * float(signal.direction)

    # Round-trip cost in bps (e.g. 5 = 0.05%)
    if cost_bps:
        r = r - (cost_bps / 10_000.0)

    # If we don't have enough future data to compute, keep NaN
    return r
