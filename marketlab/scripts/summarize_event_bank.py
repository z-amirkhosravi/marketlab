from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def q(series: pd.Series, p: float) -> float:
    s = series.dropna()
    if len(s) == 0:
        return float("nan")
    return float(s.quantile(p))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="inp", required=True, help="Input CSV from eval_event_bank.py (yearly split)")
    p.add_argument("--out", default="event_bank_yearly_summary.csv", help="Output summary CSV")
    p.add_argument("--min-n", type=int, default=30, help="Minimum conditional sample size per slice")
    args = p.parse_args()

    df = pd.read_csv(args.inp)

    # We summarize only conditional rows, and only those with enough samples.
    cond = df[df["slice"] == "conditional"].copy()

    # Drop windows/years with no observations (n==0) or too few to trust
    cond = cond[cond["n"].fillna(0).astype(int) >= args.min_n].copy()

    if cond.empty:
        raise SystemExit(f"No conditional rows remaining after --min-n {args.min_n}. Try lowering min-n.")

    # Ensure the column exists (fail fast with a clear message)
    if "sharpe_ann" not in cond.columns:
        raise SystemExit(
            "Input file has no 'sharpe_ann' column. Re-run eval_event_bank.py after adding sharpe_ann in evaluate_event()."
        )

    grp = cond.groupby(["symbol", "timeframe", "horizon", "event_spec", "event"], dropna=False)

    rows = []
    for (symbol, timeframe, horizon, event_spec, event_name), g in grp:
        means = g["mean"]
        sharpes_ann = g["sharpe_ann"]
        hit = g["hit_rate"]

        rows.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "horizon": horizon,
            "event_spec": event_spec,
            "event": event_name,

            "n_years_valid": int(g["slice_name"].nunique()),
            "avg_n": float(g["n"].mean()),
            "min_n": int(g["n"].min()),
            "max_n": int(g["n"].max()),

            "frac_years_mean_pos": float((means > 0).mean()),
            "frac_years_sharpe_ann_pos": float((sharpes_ann > 0).mean()),

            "mean_median": float(means.median()),
            "mean_p10": q(means, 0.10),
            "mean_p90": q(means, 0.90),
            "mean_worst": float(means.min()),

            "sharpe_ann_median": float(sharpes_ann.median()),
            "sharpe_ann_p10": q(sharpes_ann, 0.10),
            "sharpe_ann_p90": q(sharpes_ann, 0.90),
            "sharpe_ann_worst": float(sharpes_ann.min()),

            "hit_rate_median": float(hit.median()),
        })

    out = pd.DataFrame(rows)

    # Rank: stability first, then annualized Sharpe, then payoff, then coverage
    out = out.sort_values(
        by=["frac_years_mean_pos", "sharpe_ann_median", "mean_median", "n_years_valid"],
        ascending=[False, False, False, False],
    )

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)

    print(f"Wrote {len(out)} event summaries to {args.out}")
    with pd.option_context("display.max_columns", 60, "display.width", 160):
        print(out.head(20))


if __name__ == "__main__":
    main()
