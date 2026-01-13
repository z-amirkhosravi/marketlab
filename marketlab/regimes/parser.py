from __future__ import annotations

from marketlab.events import Event
from marketlab.regimes.registry import REGIME_FACTORIES

def parse_number(x: str):
    try:
        if "." in x or "e" in x.lower():
            return float(x)
        return int(x)
    except ValueError:
        return float(x)

def build_regime(spec: str) -> Event:
    """
    Examples:
      trend_up_200
      trend_down_200
      vol_high:0.67
      vol_low:0.33
    """
    name, *args = spec.split(":")
    if name not in REGIME_FACTORIES:
        raise ValueError(f"Unknown regime '{name}'")
    params = [parse_number(a) for a in args]
    return REGIME_FACTORIES[name](*params)
