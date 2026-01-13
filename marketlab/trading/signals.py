from __future__ import annotations
from dataclasses import dataclass
from typing import Literal

EntryStyle = Literal["next_open"]
ExitStyle = Literal["close_at_horizon"]


@dataclass(frozen=True)
class TradeSignal:
    """
    Defines how to convert an event into a trade return series.
    """
    direction: int = +1            # +1 long, -1 short
    entry: EntryStyle = "next_open"
    exit: ExitStyle = "close_at_horizon"
