from __future__ import annotations
import pandas as pd

def yearly_slices(index: pd.DatetimeIndex):
    years = sorted(set(index.year))
    for y in years:
        mask = index.year == y
        yield f"{y}", mask

def rolling_slices(index: pd.DatetimeIndex, window: int, step: int):
    # window/step in bars (for 1d, 252 ~ 1y)
    n = len(index)
    start = 0
    while start + window <= n:
        end = start + window
        label = f"{index[start].date()}→{index[end-1].date()}"
        m = pd.Series(False, index=index)
        m.iloc[start:end] = True
        yield label, m.values  # numpy bool array is fine
        start += step
