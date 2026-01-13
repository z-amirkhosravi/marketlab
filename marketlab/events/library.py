from __future__ import annotations

import pandas as pd

from marketlab.events import Event
from marketlab.features.indicators import sma
from marketlab.features.volatility import atr, true_range


def close_above_sma(n: int) -> Event:
    def _fn(df: pd.DataFrame) -> pd.Series:
        s = sma(df["close"], n)
        return df["close"] > s

    return Event(name=f"close>sma{n}", fn=_fn)


def close_below_sma(n: int) -> Event:
    def _fn(df: pd.DataFrame) -> pd.Series:
        s = sma(df["close"], n)
        return df["close"] < s

    return Event(name=f"close<sma{n}", fn=_fn)

# --- Gap events ---

def gap_up(thresh: float) -> Event:
    """
    Gap up: (open / prev_close - 1) >= thresh
    thresh is a decimal (0.01 = 1%)
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        prev_close = df["close"].shift(1)
        gap = df["open"] / prev_close - 1.0
        return gap >= thresh
    return Event(name=f"gap_up>={thresh:g}", fn=_fn)

def gap_down(thresh: float) -> Event:
    """
    Gap down: (prev_close / open - 1) >= thresh  (i.e., open <= prev_close*(1-thresh))
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        prev_close = df["close"].shift(1)
        gap = 1.0 - (df["open"] / prev_close)  # positive on gap down
        return gap >= thresh
    return Event(name=f"gap_down>={thresh:g}", fn=_fn)

# --- Range / volatility events ---

def range_expansion_atr(mult: float, atr_window: int = 14) -> Event:
    """
    True range >= mult * ATR(atr_window)
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        tr = true_range(df)
        a = atr(df, window=atr_window)
        return tr >= (mult * a)
    return Event(name=f"tr>={mult:g}*atr{atr_window}", fn=_fn)

def range_contraction_atr(mult: float, atr_window: int = 14) -> Event:
    """
    True range <= mult * ATR(atr_window)
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        tr = true_range(df)
        a = atr(df, window=atr_window)
        return tr <= (mult * a)
    return Event(name=f"tr<={mult:g}*atr{atr_window}", fn=_fn)
