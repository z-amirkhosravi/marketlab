from __future__ import annotations

from dataclasses import dataclass
import pandas as pd
from marketlab.events import Event


@dataclass(frozen=True)
class AndEvent(Event):
    left: Event
    right: Event

    def __init__(self, left: Event, right: Event, name: str | None = None):
        object.__setattr__(self, "left", left)
        object.__setattr__(self, "right", right)
        object.__setattr__(self, "name", name or f"({left.name} AND {right.name})")
        object.__setattr__(self, "fn", lambda df: left.mask(df) & right.mask(df))


@dataclass(frozen=True)
class OrEvent(Event):
    left: Event
    right: Event

    def __init__(self, left: Event, right: Event, name: str | None = None):
        object.__setattr__(self, "left", left)
        object.__setattr__(self, "right", right)
        object.__setattr__(self, "name", name or f"({left.name} OR {right.name})")
        object.__setattr__(self, "fn", lambda df: left.mask(df) | right.mask(df))


@dataclass(frozen=True)
class NotEvent(Event):
    inner: Event

    def __init__(self, inner: Event, name: str | None = None):
        object.__setattr__(self, "inner", inner)
        object.__setattr__(self, "name", name or f"(NOT {inner.name})")
        object.__setattr__(self, "fn", lambda df: ~inner.mask(df))


def shifted(event: Event, periods: int, name: str | None = None) -> Event:
    def _fn(df: pd.DataFrame) -> pd.Series:
        return event.mask(df).shift(periods).fillna(False)

    return Event(name=name or f"{event.name}.shift({periods})", fn=_fn)


def rolling_any(event: Event, window: int, name: str | None = None) -> Event:
    """
    True at t if event was true at least once in the last `window` bars (including t).
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        m = event.mask(df).astype(int)
        return m.rolling(window=window, min_periods=1).max().astype(bool)

    return Event(name=name or f"any_{window}({event.name})", fn=_fn)


def rolling_count_ge(event: Event, window: int, k: int, name: str | None = None) -> Event:
    """
    True at t if event was true at least k times in the last `window` bars.
    """
    def _fn(df: pd.DataFrame) -> pd.Series:
        m = event.mask(df).astype(int)
        return (m.rolling(window=window, min_periods=window).sum() >= k).fillna(False)

    return Event(name=name or f"count_{window}>={k}({event.name})", fn=_fn)
