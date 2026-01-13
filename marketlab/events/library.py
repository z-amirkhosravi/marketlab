from __future__ import annotations

import pandas as pd

from marketlab.events import Event
from marketlab.features.indicators import sma


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
