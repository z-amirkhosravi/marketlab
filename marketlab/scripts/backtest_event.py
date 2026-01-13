from __future__ import annotations

import argparse
import pandas as pd

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, read_bars
from marketlab.events.parser import build_event
from marketlab.regimes import build_regime
from marketlab.events import AndEvent
from marketlab.trading.signals import TradeSignal
from marketlab.trading.returns import trade_returns_next_open_close_at_horizon
from marketlab.backtest.simple import backtest_from_signal_returns

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", default="SPY")
    p.add_argument("--timeframe", default="1d")
    p.add_argument("--horizon", type=int, default=1)
    p.add_argument("--event", required=True)
    p.add_argument("--regime", default=None)
    p.add_argument("--direction", choices=["long", "short"], default="long")
    p.add_argument("--cost-bps", type=float, default=0.0)
    p.add_argument("--out", default=None, help="Optional CSV output for the equity curve")
    args = p.parse_args()

    cfg = MarketlabConfig()
    lib = get_lib(get_arctic(cfg.arctic_uri), cfg.daily_lib)
    df = read_bars(lib, args.timeframe, args.symbol).copy().sort_index()

    e = build_event(args.event)
    if args.regime:
        r = build_regime(args.regime)
        e = AndEvent(e, r, name=f"({e.name} AND {r.name})")

    mask = e.mask(df)

    sig = TradeSignal(direction=+1 if args.direction == "long" else -1)
    tr = trade_returns_next_open_close_at_horizon(df, horizon=args.horizon, signal=sig, cost_bps=args.cost_bps)

    bt = backtest_from_signal_returns(mask, tr)

    # Basic stats
    total_return = bt["equity"].iloc[-1] - 1.0
    n_trades = int(bt["taken"].sum())
    avg_trade = bt.loc[bt["taken"], "trade_return"].mean()
    hit_rate = (bt.loc[bt["taken"], "trade_return"] > 0).mean()

    print({
        "symbol": args.symbol,
        "event": e.name,
        "horizon": args.horizon,
        "direction": args.direction,
        "cost_bps": args.cost_bps,
        "n_trades": n_trades,
        "avg_trade": float(avg_trade) if pd.notna(avg_trade) else None,
        "hit_rate": float(hit_rate) if pd.notna(hit_rate) else None,
        "total_return": float(total_return),
    })

    if args.out:
        bt.to_csv(args.out, index=True)
        print(f"Wrote equity curve to {args.out}")

if __name__ == "__main__":
    main()
