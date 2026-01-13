from __future__ import annotations

import pandas as pd

from marketlab.events import Event
from marketlab.features.indicators import sma
from marketlab.features.volatility import atr


def trend_up_200() -> Event:
    def _fn(df: pd.DataFrame) -> pd.Series:
        s = sma(df["close"], 200)
        return df["close"] > s
    return Event(name="trend_up_200", fn=_fn)


def trend_down_200() -> Event:
    def _fn(df: pd.DataFrame) -> pd.Series:
        s = sma(df["close"], 200)
        return df["close"] < s
    return Event(name="trend_down_200", fn=_fn)


def vol_ratio(window_fast: int = 20, window_slow: int = 252) -> Event:
    """
    Not a mask by itself, but we'll use this ratio inside vol_high/vol_low.
    Included here as a helper.
    """
    def _ratio(df: pd.DataFrame) -> pd.Series:
        a_fast = atr(df, window=window_fast)
        a_slow = atr(df, window=window_slow)
        return a_fast / a_slow
    return Event(name=f"atr{window_fast}_over_atr{window_slow}", fn=_ratio)  # returns float series


def vol_high(q: float = 0.67, window_fast: int = 20, window_slow: int = 252) -> Event:
    """
    True when ATRfast/ATRslow is in the top q-quantile (e.g. 0.67 ~ top tercile).
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        ratio = atr(df, window_fast) / atr(df, window_slow)
        thresh = ratio.quantile(q)
        return ratio >= thresh
    return Event(name=f"vol_high_q{q:g}", fn=_fn)


def vol_low(q: float = 0.33, window_fast: int = 20, window_slow: int = 252) -> Event:
    """
    True when ATRfast/ATRslow is in the bottom q-quantile (e.g. 0.33 ~ bottom tercile).
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        ratio = atr(df, window_fast) / atr(df, window_slow)
        thresh = ratio.quantile(q)
        return ratio <= thresh
    return Event(name=f"vol_low_q{q:g}", fn=_fn)
