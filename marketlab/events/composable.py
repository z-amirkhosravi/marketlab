# marketlab/events/composable.py
from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from .base import Event

@dataclass(frozen=True)
class AndEvent:
    left: Event
    right: Event
    name: str = ""

    def __post_init__(self):
        if not self.name:
            object.__setattr__(self, "name", f"({self.left.name} AND {self.right.name})")

    def mask(self, df: pd.DataFrame) -> pd.Series:
        return self.left.mask(df) & self.right.mask(df)

@dataclass(frozen=True)
class OrEvent:
    left: Event
    right: Event
    name: str = ""

    def __post_init__(self):
        if not self.name:
            object.__setattr__(self, "name", f"({self.left.name} OR {self.right.name})")

    def mask(self, df: pd.DataFrame) -> pd.Series:
        return self.left.mask(df) | self.right.mask(df)

@dataclass(frozen=True)
class NotEvent:
    inner: Event
    name: str = ""

    def __post_init__(self):
        if not self.name:
            object.__setattr__(self, "name", f"(NOT {self.inner.name})")

    def mask(self, df: pd.DataFrame) -> pd.Series:
        return ~self.inner.mask(df)

@dataclass(frozen=True)
class ShiftedEvent:
    inner: Event
    periods: int
    name: str = ""

    def __post_init__(self):
        if not self.name:
            object.__setattr__(self, "name", f"{self.inner.name}.shift({self.periods})")

    def mask(self, df: pd.DataFrame) -> pd.Series:
        return self.inner.mask(df).shift(self.periods).fillna(False)
