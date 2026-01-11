from __future__ import annotations

import datetime as dt
from pathlib import Path
import pandas as pd

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, key_bars

def flatfile_path(cfg: MarketlabConfig, day: dt.date) -> Path:
    return (
        cfg.massive_cache_dir
        / cfg.daily_symbol_set
        / f"{day.year:04d}"
        / f"{day.month:02d}"
        / f"{day:%Y-%m-%d}.csv.gz"
    )

def ingest_day(cfg: MarketlabConfig, day: dt.date, *, append: bool = True) -> dict:
    path = flatfile_path(cfg, day)
    if not path.exists():
        raise FileNotFoundError(path)

    df = pd.read_csv(path, compression="gzip")

    required = {"ticker", "volume", "open", "close", "high", "low", "window_start"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns {sorted(missing)} in {path}")

    # window_start is epoch in *nanoseconds*
    ts = pd.to_datetime(df["window_start"], unit="ns", utc=True)
    df = df.assign(timestamp=ts).set_index("timestamp")

    arctic = get_arctic(cfg.arctic_uri)
    lib = get_lib(arctic, cfg.daily_lib)

    rows_total = 0
    symbols = 0

    # group by ticker and append to bars/1d/{symbol}
    for sym, g in df.groupby("ticker", sort=False):
        out = g[["open", "high", "low", "close", "volume"]].sort_index()

        k = key_bars("1d", sym)
        if append:
            lib.append(k, out)
        else:
            lib.write(k, out, prune_previous_versions=True)

        rows_total += len(out)
        symbols += 1

    return {"date": str(day), "file": str(path), "symbols": symbols, "rows_total": rows_total}
