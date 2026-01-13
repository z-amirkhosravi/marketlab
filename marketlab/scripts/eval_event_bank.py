from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import pandas as pd

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, read_bars
from marketlab.events.parser import build_event
from marketlab.outcomes.forward import fwd_return
from marketlab.research.evaluate import evaluate_event
from marketlab.research.splits import yearly_slices, rolling_slices


def load_event_specs(events_file: str | None, events: list[str]) -> list[str]:
    specs: list[str] = []
    specs.extend(events or [])
    if events_file:
        p = Path(events_file)
        if not p.exists():
            raise FileNotFoundError(p)
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            specs.append(line)
    # de-dup while preserving order
    seen = set()
    out = []
    for s in specs:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="SPY")
    p.add_argument("--timeframe", default="1d")
    p.add_argument("--horizon", type=int, default=1, help="Forward bars")
    p.add_argument("--split", default="none", help="none | yearly | rolling:<window>:<step>")
    p.add_argument("--event", action="append", default=[], help="Event spec (repeatable)")
    p.add_argument("--events-file", default=None, help="Path to a text file of event specs (one per line)")
    p.add_argument("--out", default="event_bank_results.csv", help="Output CSV path")
    p.add_argument("--limit-print", type=int, default=20, help="How many rows to print as preview")
    args = p.parse_args()

    event_specs = load_event_specs(args.events_file, args.event)
    if not event_specs:
        raise ValueError("No events provided. Use --event ... or --events-file ...")

    cfg = MarketlabConfig()
    lib = get_lib(get_arctic(cfg.arctic_uri), cfg.daily_lib)
    df = read_bars(lib, args.timeframe, args.symbol).copy().sort_index()

    # outcome series (same for all events)
    r = fwd_return(df["close"], horizon=args.horizon)

    # choose slices
    slices: list[tuple[str, pd.Series | list[bool]]] = []
    if args.split == "none":
        slices = [("all", pd.Series(True, index=df.index))]
    elif args.split == "yearly":
        slices = list(yearly_slices(df.index))
    elif args.split.startswith("rolling:"):
        _, w, s = args.split.split(":")
        w, s = int(w), int(s)
        slices = list(rolling_slices(df.index, window=w, step=s))
    else:
        raise ValueError("Invalid --split")

    all_rows = []
    started = dt.datetime.now()

    for spec in event_specs:
        event = build_event(spec)
        event_mask_full = event.mask(df)

        for slice_name, idx_mask in slices:
            dd = df.loc[idx_mask]
            mm = event_mask_full.loc[idx_mask]
            rr = r.loc[idx_mask]

            out = evaluate_event(dd, mm, rr, timeframe=args.timeframe, horizon=args.horizon)
            out.insert(0, "symbol", args.symbol)
            out.insert(1, "timeframe", args.timeframe)
            out.insert(2, "horizon", args.horizon)
            out.insert(3, "event_spec", spec)
            out.insert(4, "event", event.name)
            out.insert(5, "slice_name", slice_name)

            all_rows.append(out)

    result = pd.concat(all_rows, ignore_index=True)

    # Save
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(args.out, index=False)

    elapsed = dt.datetime.now() - started
    print(f"Wrote {len(result)} rows to {args.out} in {elapsed}.")

    # Preview: show conditional rows only, sorted by sharpe
    preview = result[result["slice"] == "conditional"].copy()
    preview = preview.sort_values(["slice_name", "sharpe"], ascending=[True, False])
    with pd.option_context("display.max_columns", 60, "display.width", 160):
        print(preview.head(args.limit_print))


if __name__ == "__main__":
    main()
