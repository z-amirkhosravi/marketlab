from __future__ import annotations
from typing import Callable, Dict

from marketlab.events import Event
from marketlab.regimes.library import trend_up_200, trend_down_200, vol_high, vol_low

REGIME_FACTORIES: Dict[str, Callable[..., Event]] = {
    "trend_up_200": trend_up_200,
    "trend_down_200": trend_down_200,
    "vol_high": vol_high,   # expects q (float) optionally
    "vol_low": vol_low,     # expects q (float) optionally
}
