from __future__ import annotations

import argparse
import pandas as pd

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, read_bars
from marketlab.outcomes.forward import fwd_return
from marketlab.research.evaluate import evaluate_event
from marketlab.events.library import close_above_sma
from marketlab.events.composable import AndEvent
from marketlab.events.parser import build_event
from marketlab.regimes import build_regime
from marketlab.events import AndEvent
from marketlab.trading.signals import TradeSignal
from marketlab.trading.returns import trade_returns_next_open_close_at_horizon

from marketlab.research.splits import yearly_slices, rolling_slices
import numpy as np

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="SPY")
    p.add_argument("--timeframe", default="1d")
    p.add_argument("--horizon", type=int, default=1, help="Forward bars (1 = next day for 1d data)")
    p.add_argument(
    "--event",
    required=True,
    help="Event spec, e.g. close_above_sma:20 or close_above_sma:20&close_above_sma:50"
    )
    p.add_argument("--split", default="none", help="none | yearly | rolling:<window>:<step>")
    p.add_argument("--regime", default=None, help="Optional regime spec, e.g. trend_up_200 or vol_high:0.67")
    p.add_argument("--trade", action="store_true", help="Evaluate a trade: enter next open, exit close at horizon")
    p.add_argument("--direction", choices=["long", "short"], default="long")
    p.add_argument("--cost-bps", type=float, default=0.0, help="Round-trip cost per trade in bps (only when --trade)")



    args = p.parse_args()

    cfg = MarketlabConfig()
    lib = get_lib(get_arctic(cfg.arctic_uri), cfg.daily_lib)

    df = read_bars(lib, args.timeframe, args.symbol).copy()
    df = df.sort_index()

    # df["sma"] = sma(df["close"], args.sma)
    # event_mask = df["close"] > df["sma"]

    event = build_event(args.event)
    event_mask = event.mask(df) 

    regime = None
    if args.regime:
        regime = build_regime(args.regime)
        combined = AndEvent(event, regime, name=f"({event.name} AND {regime.name})")
        event = combined
        event_mask = event.mask(df)


    # outcome series
    if args.trade:
        sig = TradeSignal(direction=+1 if args.direction == "long" else -1)
        r = trade_returns_next_open_close_at_horizon(df, horizon=args.horizon, signal=sig, cost_bps=args.cost_bps)
    else:
        r = fwd_return(df["close"], horizon=args.horizon)

    outs = []

    def eval_one(slice_name, idx_mask):
        dd = df.loc[idx_mask]
        mm = event_mask.loc[idx_mask]
        rr = r.loc[idx_mask]
        out = evaluate_event(dd, mm, rr, timeframe=args.timeframe, horizon=args.horizon)
        out.insert(0, "slice_name", slice_name)
        return out

    if args.split == "none":
        out = evaluate_event(dd, mm, rr, timeframe=args.timeframe, horizon=args.horizon)
        out.insert(0, "slice_name", "all")
        outs.append(out)

    elif args.split == "yearly":
        for name, m in yearly_slices(df.index):
            outs.append(eval_one(name, m))

    elif args.split.startswith("rolling:"):
        _, w, s = args.split.split(":")
        w, s = int(w), int(s)
        for name, m in rolling_slices(df.index, window=w, step=s):
            outs.append(eval_one(name, m))

    else:
        raise ValueError("Invalid --split")

    out = pd.concat(outs, ignore_index=True)

    out.insert(0, "symbol", args.symbol)
    out.insert(1, "timeframe", args.timeframe)
    out.insert(2, "horizon", args.horizon)
    out.insert(3, "event", event.name)
    out.insert(4, "regime", regime.name if regime else "none")
    # Pretty print
    with pd.option_context("display.max_columns", 50, "display.width", 140):
        print(out)

if __name__ == "__main__":
    main()
