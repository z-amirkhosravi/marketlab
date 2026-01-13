from __future__ import annotations
from typing import Callable, Dict

from marketlab.events import Event
from marketlab.events.library import (
    close_above_sma,
    close_below_sma,
)

EVENT_FACTORIES: Dict[str, Callable[..., Event]] = {
    "close_above_sma": close_above_sma,
    "close_below_sma": close_below_sma,
}
