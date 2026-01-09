# marketlab/outcomes/forward.py
from __future__ import annotations
import pandas as pd
import numpy as np

def fwd_return(close: pd.Series, horizon: int) -> pd.Series:
    """Simple forward return over `horizon` bars."""
    return close.shift(-horizon) / close - 1.0

def fwd_log_return(close: pd.Series, horizon: int) -> pd.Series:
    return np.log(close.shift(-horizon) / close)

def direction_label(close: pd.Series, horizon: int, flat_eps: float = 0.0005) -> pd.Series:
    r = fwd_return(close, horizon)
    lab = pd.Series(index=close.index, dtype="object")
    lab[r > flat_eps] = "up"
    lab[r < -flat_eps] = "down"
    lab[(r >= -flat_eps) & (r <= flat_eps)] = "flat"
    return lab
