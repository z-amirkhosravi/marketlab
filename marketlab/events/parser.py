from __future__ import annotations

from marketlab.events import Event, AndEvent, OrEvent, NotEvent
from marketlab.events.registry import EVENT_FACTORIES


def build_event(spec: str) -> Event:
    """
    Examples:
      close_above_sma:20
      close_below_sma:50
      close_above_sma:20&close_above_sma:50
      !close_below_sma:20
    """

    def parse_atom(atom: str) -> Event:
        neg = atom.startswith("!")
        atom = atom[1:] if neg else atom

        name, *args = atom.split(":")
        if name not in EVENT_FACTORIES:
            raise ValueError(f"Unknown event '{name}'")

        params = [int(a) for a in args]
        e = EVENT_FACTORIES[name](*params)
        return NotEvent(e) if neg else e

    if "&" in spec:
        parts = spec.split("&")
        e = parse_atom(parts[0])
        for p in parts[1:]:
            e = AndEvent(e, parse_atom(p))
        return e

    if "|" in spec:
        parts = spec.split("|")
        e = parse_atom(parts[0])
        for p in parts[1:]:
            e = OrEvent(e, parse_atom(p))
        return e

    return parse_atom(spec)
