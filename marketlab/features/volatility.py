from __future__ import annotations
import pandas as pd

def true_range(df: pd.DataFrame) -> pd.Series:
    """
    True Range: max(high-low, abs(high-prev_close), abs(low-prev_close))
    Assumes df has columns: high, low, close
    """
    prev_close = df["close"].shift(1)
    hl = df["high"] - df["low"]
    hc = (df["high"] - prev_close).abs()
    lc = (df["low"] - prev_close).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr

def atr(df: pd.DataFrame, window: int = 14) -> pd.Series:
    """
    Simple ATR (SMA of True Range). Good enough for v1.
    """
    return true_range(df).rolling(window=window, min_periods=window).mean()
