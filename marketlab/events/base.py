# marketlab/events/base.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol
import pandas as pd

class Event(Protocol):
    name: str
    def mask(self, df: pd.DataFrame) -> pd.Series:
        """Return boolean Series indexed like df.index"""

@dataclass(frozen=True)
class FuncEvent:
    name: str
    fn: callable

    def mask(self, df: pd.DataFrame) -> pd.Series:
        m = self.fn(df)
        if not isinstance(m, pd.Series):
            raise TypeError("Event function must return a pd.Series")
        if not m.index.equals(df.index):
            raise ValueError("Event mask must share df.index")
        return m.astype(bool)
