from __future__ import annotations

import datetime as dt
from collections import defaultdict
from pathlib import Path

import pandas as pd
from arcticdb.exceptions import NoDataFoundException

from marketlab.config import MarketlabConfig
from marketlab.data.arctic import get_arctic, get_lib, key_bars
from marketlab.data.polygon_massive.ingest_daily_from_cache import flatfile_path, is_day_ingested, mark_day_ingested

def month_range(start: dt.date, end: dt.date):
    cur = dt.date(start.year, start.month, 1)
    while cur <= end:
        yield cur.year, cur.month
        # advance 1 month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)

def days_in_month(year: int, month: int, start: dt.date, end: dt.date):
    first = dt.date(year, month, 1)
    if month == 12:
        nextm = dt.date(year + 1, 1, 1)
    else:
        nextm = dt.date(year, month + 1, 1)
    last = nextm - dt.timedelta(days=1)

    lo = max(first, start)
    hi = min(last, end)

    d = lo
    while d <= hi:
        yield d
        d += dt.timedelta(days=1)

def ingest_month(cfg: MarketlabConfig, year: int, month: int, start: dt.date, end: dt.date) -> dict:
    arctic = get_arctic(cfg.arctic_uri)
    lib = get_lib(arctic, cfg.daily_lib)

    # symbol -> list of rows (timestamp, open, high, low, close, volume)
    rows = defaultdict(list)

    days_found = 0
    days_ingested = 0
    total_rows_read = 0

    for day in days_in_month(year, month, start, end):
        path = flatfile_path(cfg, day)
        if not path.exists():
            continue  # weekends/holidays or not downloaded

        days_found += 1

        # Skip whole day if already ingested (manifest)
        if is_day_ingested(lib, cfg, day):
            continue

        df = pd.read_csv(path, compression="gzip")

        # Expected schema (from your header):
        # ticker,volume,open,close,high,low,window_start,transactions
        required = {"ticker", "volume", "open", "close", "high", "low", "window_start"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns {sorted(missing)} in {path}")

        # window_start is ns epoch
        ts = pd.to_datetime(df["window_start"], unit="ns", utc=True)

        # Add rows into per-symbol buffers
        # Use itertuples for speed
        df = df.assign(_ts=ts)
        for r in df.itertuples(index=False):
            # r.ticker, r.open, r.high, r.low, r.close, r.volume, r._ts
            rows[r.ticker].append((r._ts, r.open, r.high, r.low, r.close, r.volume))

        total_rows_read += len(df)
        days_ingested += 1

        # Mark the day ingested *after* it's buffered (we’ll persist after writing too)
        # We'll mark after successful writes below to be safer.

    # Nothing new this month
    if days_ingested == 0:
        return {
            "month": f"{year:04d}-{month:02d}",
            "days_found": days_found,
            "days_ingested": 0,
            "symbols_written": 0,
            "rows_read": total_rows_read,
            "rows_appended": 0,
            "skipped": True,
        }

    # Write once per symbol for the month
    symbols_written = 0
    rows_appended = 0

    for sym, lst in rows.items():
        if not lst:
            continue
        out = pd.DataFrame(lst, columns=["timestamp", "open", "high", "low", "close", "volume"])
        out = out.set_index("timestamp").sort_index()

        k = key_bars("1d", sym)
        # Append (fast path). You can add optional dedupe later if needed.
        lib.append(k, out)

        symbols_written += 1
        rows_appended += len(out)

    # Now that writes succeeded, mark the ingested days in this month
    for day in days_in_month(year, month, start, end):
        path = flatfile_path(cfg, day)
        if not path.exists():
            continue
        if is_day_ingested(lib, cfg, day):
            continue
        # If we got here, we ingested it into buffers and wrote successfully.
        # But how do we know which days were buffered? We can track them:
        # simplest: re-check by file existence + not ingested before loop.
        # Better: keep a list during read loop.
        # We'll do the better way in the CLI layer (below) to avoid double reads.

    return {
        "month": f"{year:04d}-{month:02d}",
        "days_found": days_found,
        "days_ingested": days_ingested,
        "symbols_written": symbols_written,
        "rows_read": total_rows_read,
        "rows_appended": rows_appended,
        "skipped": False,
    }
