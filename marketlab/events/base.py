from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import pandas as pd


@dataclass(frozen=True)
class Event:
    name: str
    fn: Callable[[pd.DataFrame], pd.Series]

    def mask(self, df: pd.DataFrame) -> pd.Series:
        m = self.fn(df)
        if not isinstance(m, pd.Series):
            raise TypeError("Event function must return a pandas Series")
        if not m.index.equals(df.index):
            raise ValueError("Event mask index must match df.index")
        return m.astype(bool)
